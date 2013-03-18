#! /usr/bin/perl
use strict;
use warnings;
use DBIx::Simple;
use Time::Piece;

my $mysql_host = $ARGV[0];
my $mysql_schema = $ARGV[1];
my $mysql_table = $ARGV[2];
my $column_whitelist = $ARGV[3];# "column1,column2"
my $debug = 0;

sub usage {
    print "usage: ./mysqlbridge.pl [IP] [SCHEMA] [TABLE_NAME]\n";
    print "   or: ./mysqlbridge.pl [IP] [SCHEMA] [TABLE_NAME] \"column1,column2\" <- filter columns, whitelist\n";
    exit 0;
}

if(!$mysql_host || !$mysql_schema || !$mysql_table){
    &usage;
}

my @arr_columns_whitelist = [];
my $key_endfix = "";
if(defined $column_whitelist){
    @arr_columns_whitelist = split ",",$column_whitelist;
    if(scalar @arr_columns_whitelist == 0){
        &usage;
    }else{
        $key_endfix = "_filtered";
        for(@arr_columns_whitelist){
            if($_!~/^[a-z0-9_\-]+$/){
                &usage;
            }
        }
        map { $_ = "\"$_\""; } @arr_columns_whitelist;
    }
}

my $blackhole_tablename = 'BH';
my $mysql_user = 'root';

print "root's password please: ";
my $mysql_passwd = <STDIN>;
chomp $mysql_passwd;

my $mysql_domain = '%';
my $mysql_port = 3306;
my $mysql_func_prefix = 'charge_null_';

my $use_event = 0;
my $mysql_event_check_sec = 60;
my $mysql_event_check_timetype = 'SECOND';

sub run_mysql {
    my $tmpname = shift;
    my $sql = shift;
    return if (!$tmpname || $tmpname !~ /^[a-zA-Z0-9_]+$/ || !$sql);
    if(!$debug){
        open my $fh,">/tmp/$tmpname.sql";
        print $fh $sql;
        close $fh;
        `mysql -u$mysql_user -p$mysql_passwd -h$mysql_host $mysql_schema -e 'source /tmp/$tmpname.sql'`;
        `rm -f /tmp/$tmpname.sql`;
    }else{
        print "_"x50;
        print "\n";
        print $sql."\n";
        print "_"x50;
        print "\n";
    }
}
sub connect_db {
    my @dsn = ("dbi:mysql:host=".$mysql_host.";port=".$mysql_port.";database=".$mysql_schema.";",$mysql_user,$mysql_passwd,{ RaiseError => 1,AutoCommit => 1 },);
    my $dbconn = DBIx::Simple->connect(@dsn);
    $dbconn->query("SET NAMES utf8");
    return $dbconn;
}

sub init_black_hole_table {
    my $sql = 'CREATE TABLE IF NOT EXISTS `'.$mysql_schema.'`.`'.$blackhole_tablename.'` (
        `data` bit(1) NOT NULL
    ) ENGINE=`BLACKHOLE`;';
    my $db = &connect_db;
    $db->query($sql);
    $db->disconnect;
}

sub parse_schema {
    my $sql;
    if(scalar @arr_columns_whitelist>0){
        $sql = 'select group_concat(column_name) from information_schema.columns where table_schema="'.$mysql_schema.'" and table_name="'.$mysql_table.'" and (column_name in ('.( join ",", @arr_columns_whitelist ).') or column_key = "PRI") order by ordinal_position;';
    }else{
        $sql = 'select group_concat(column_name) from information_schema.columns where table_schema="'.$mysql_schema.'" and table_name="'.$mysql_table.'" order by ordinal_position;';
    }
    my $db = &connect_db;
    my $result = $db->query($sql);
    if($result){
        my $info = $result->array;
        my $columns = $info->[0];
        my $key = $mysql_schema.'_'.$mysql_table;
        $db->query('select redis("hdel schema '.$key.$key_endfix.'")');
        $db->query('select redis("hset schema '.$key.$key_endfix.' '.$columns.'");');
        $db->disconnect;
    }else{
        print "error!!\n";
        exit 1;
    }
    return undef;
}

sub parse_table {
    my $type = shift || 0; #0 function 1 ins_trigger 2 upd_trigger 3 del_trigger
    my $sql;
    if(scalar @arr_columns_whitelist>0){
        $sql = 'select * from information_schema.columns where table_schema="'.$mysql_schema.'" and table_name="'.$mysql_table.'" and (column_name in ('.( join ",", @arr_columns_whitelist ).') or column_key = "PRI") order by ordinal_position;';
    }else{
        $sql = 'select * from information_schema.columns where table_schema="'.$mysql_schema.'" and table_name="'.$mysql_table.'" order by ordinal_position;';
    }
    my $db = &connect_db;
    my $result = $db->query($sql);
    my $info = $result->hashes;
    $db->disconnect;

    my $pri_columns = [];
    my $nrm_columns = [];

    for my $obji (@$info){
        my $column_name = (($type==1 || $type==2) ? "NEW." : ($type==0?"":"OLD.")) . "`$obji->{column_name}`";
        $column_name = "ifnull(".$column_name.",'')" if($obji->{is_nullable} eq "YES");
        $column_name = "unix_timestamp($column_name)" if($obji->{data_type} eq "timestamp" || $obji->{data_type} eq "datetime");
        $obji->{column_name} = $column_name;
        if($obji->{column_key} eq "PRI"){
            push @$pri_columns,$obji->{column_name} 
        }else{
            push @$nrm_columns,$obji->{column_name}.",";
        }
    }

    my $func = "";
    my $pri_str = join ',":",',@$pri_columns;
    my $nrm_str = join "':',",@$nrm_columns;
    $nrm_str =~ s/,$//;
    if($type == 0){
       $func = 'insert into '.$blackhole_tablename.' (select redis(concat(concat("hset '.$mysql_table.$key_endfix.' ",'.$pri_str.'," "),to_base64(concat('.$nrm_str.')))) from '.$mysql_table.');';
    }elsif($type == 1 || $type == 2){
       $func = 'insert into '.$blackhole_tablename.' select redis(concat(concat("hset '.$mysql_table.$key_endfix.' ",'.$pri_str.'," "),to_base64(concat('.$nrm_str.'))));';
    }elsif($type == 3){
       $func = 'insert into '.$blackhole_tablename.' select redis(concat("hdel '.$mysql_table.$key_endfix.' ",'.$pri_str.'));';
    }
    return $func;
}
sub create_function {
    my $sql = shift;
    return if(!$sql);
    my $name = $mysql_func_prefix . $mysql_table . $key_endfix;
    my $func_sql = <<SQL;
drop procedure if exists `##schema`.`##name`;
DELIMITER ;;
CREATE DEFINER = `##schema`@`##domain` PROCEDURE `##schema`.`##name`() 
DETERMINISTIC 
SQL SECURITY INVOKER 
begin
    declare _cnt int(9) default 0;
    select ifnull(redis("hlen ##table##keyendfix"),0) into _cnt;
    if (_cnt = 0) then
        ##sql 
    end if;
end;;
DELIMITER ;
SQL
    $func_sql=~s/##domain/$mysql_domain/gm;
    $func_sql=~s/##schema/$mysql_schema/gm;
    $func_sql=~s/##name/$name/gm;
    $func_sql=~s/##table/$mysql_table/gm;
    $func_sql=~s/##keyendfix/$key_endfix/gm;
    $func_sql=~s/##sql/$sql/gm;
    &run_mysql("procedure",$func_sql);
}
sub ok {
    print " ok\n" if(!$debug);
}
sub pri {
    my $str = shift;
    print $str if (!$debug);
}

sub create_trigger{
    my $sql = shift;
    my $is_before = shift || 0;
    my $iud = shift||1;# 1 insert 2 update 3 delete
    return if(!$sql);
    my $func_sql = <<TRI;
DELIMITER ;;

drop trigger if exists `##schema`.`##name`;
CREATE TRIGGER `##schema`.`##name` ##before ##iud ON ##table 
  FOR EACH ROW
    ##sql
;;

DELIMITER ;

TRI
    if($iud==1){
        $iud="INSERT";
    }elsif($iud==2){
        $iud="UPDATE";
    }elsif($iud==3){
        $iud="DELETE";
    }
    $is_before = $is_before ? "BEFORE" : "AFTER";
    my $name = $mysql_table."_".lc($is_before)."_".lc($iud);
    $func_sql =~ s/##schema/$mysql_schema/gm;
    $func_sql =~ s/##name/$name/gm;
    $func_sql =~ s/##before/$is_before/gm;
    $func_sql =~ s/##iud/$iud/gm; 
    $func_sql =~ s/##table/$mysql_table/gm; 
    $func_sql =~ s/##sql/$sql/gm; 
    &run_mysql("trigger",$func_sql);
}

sub create_event {
    my $time = shift;
    my $long = shift;
    my $sql = shift;
    return if(!$time || !$long || !$sql || $time !~ /^\d+$/);
    my $func_sql = <<EVE;
drop event if exists `##schema`.`##name`;
DELIMITER ;;
CREATE DEFINER = `##user`@`##domain` EVENT `##schema`.`##name`
ON SCHEDULE EVERY ##time ##long
STARTS '##starts'
ON COMPLETION PRESERVE
ENABLE
DO
begin
    ##sql
end
;;
DELIMITER ;
EVE
    $func_sql=~s/##domain/$mysql_domain/gm;
    $func_sql =~ s/##user/$mysql_user/gm;
    $func_sql =~ s/##schema/$mysql_schema/gm;
    my $name = $mysql_func_prefix.$mysql_table;
    $func_sql =~ s/##name/$name/gm;
    $func_sql =~ s/##time/$time/gm;
    $long = uc($long);
    $func_sql =~ s/##long/$long/gm;
    my $t = localtime;
    my $starts = $t->ymd." ".$t->hms;
    $func_sql =~ s/##starts/$starts/gm;
    $func_sql =~ s/##sql/$sql/gm;
    &run_mysql("event",$func_sql);
}

pri "prepare table schema...";
&parse_schema;
&ok;

pri "prepare the blackhole space...";
&init_black_hole_table;
&ok;

pri "prepare function...";
my $func_cnt = &parse_table;
&create_function($func_cnt);
&ok;

my $tri_cnt;

pri "prepare insert trigger...";
$tri_cnt=&parse_table(1);
&create_trigger($tri_cnt,0,1);
&ok;

pri "prepare update trigger...";
$tri_cnt=&parse_table(2);
&create_trigger($tri_cnt,0,2);
&ok;

pri "prepare delete trigger...";
$tri_cnt=&parse_table(3);
&create_trigger($tri_cnt,0,3);
&ok;

if($use_event){
    pri "prepare event trigger...";
    &create_event($mysql_event_check_sec,$mysql_event_check_timetype,"call $mysql_func_prefix$mysql_table();");
    &ok;
}

