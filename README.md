redis-udf
=========

the mysql's udf for redis

gcc -fPIC -Wall -I/usr/include/mysql/ -I/usr/local/include/hiredis -lhiredis -shared -o redis.so redis.c

cp redis.so /usr/lib64/mysql/plugin/ && ldconfig

mysql -uroot -p -e "drop function if exists redis;create function redis returns string soname 'redis.so';"

HOW TO:

mysql > select redis("info");

mysql > select redis("set foo bar");
