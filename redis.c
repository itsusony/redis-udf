/*
 * gcc -fPIC -Wall -I/usr/include/mysql/ -I/usr/local/include/hiredis -lhiredis -shared -o redis.so redis.c
 * cp redis.so /usr/lib64/mysql/plugin/ && ldconfig
 * mysql -uroot -p -e "drop function if exists redis;create function redis returns string soname 'redis.so';"
*/
#include <stdio.h>
#include <stdlib.h>
#include <mysql.h>
#include <string.h>
#include <hiredis.h>
#define UNIXSOCKET "/var/tmp/redis.sock"

my_bool redis_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {

    redisContext *rdsCon;
    if(args->arg_count!=1 || args->lengths[0]==0 || args->arg_type[0]!=STRING_RESULT || args->args[0]==NULL || args->args[0][0]==' '){
        strcpy(message, "redis() requires ONE string arguments");
        return 1;
    }   

    initid->maybe_null = 1;
    initid->ptr = NULL;

    /* Connect to Redis */
    rdsCon = redisConnectUnix(UNIXSOCKET);
    if(rdsCon->err){
        redisFree(rdsCon);
        strcpy(message, "redis() connect to db failed");
        return 1;
    }   

    initid->ptr = (char*)rdsCon;
    return 0;
}

void redis_deinit(UDF_INIT *initid) {
    if(initid->ptr!=NULL){
        redisContext *rdsCon;
        rdsCon = (redisContext*)initid->ptr;
        redisFree(rdsCon);
    }   
}

char *redis(UDF_INIT *initid, UDF_ARGS *args,
    char *result, unsigned long *length,
    char *is_null, char *error)
{
    redisContext* rdsCon = (redisContext*)initid->ptr;
    result[0] = '\0';
    *length = 0;
    redisReply *rdsRply = redisCommand(rdsCon, args->args[0]);

    if(!rdsRply){
        *is_null=1;
    }else{
        switch(rdsRply->type) {
            case REDIS_REPLY_ERROR:
                *error = 1;
                break;

            case REDIS_REPLY_ARRAY: {
                int elmLen;
                int bufIdx = 0;
                int i;

                for(i=0; i<rdsRply->elements; i++){
                    elmLen = rdsRply->element[i]->len;
                    memcpy(&result[bufIdx], rdsRply->element[i]->str, elmLen);
                    result[bufIdx + elmLen] = '\n';
                    bufIdx += (elmLen + 1);
                }

                result[bufIdx - 1] = '\0';
                *length = bufIdx - 1;
                break;
            }
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
                if(rdsRply->len > 0){
                    strcpy(result, rdsRply->str);
                    *length = rdsRply->len;
                }else{
                    *is_null = 1;
                }
                break;

            case REDIS_REPLY_INTEGER:
                sprintf(result, "%lld", rdsRply->integer);
                *length = strlen(result);
                break;

            case REDIS_REPLY_NIL:
                *is_null = 1;
                break;
        }
    }

    freeReplyObject(rdsRply);
    return result;
}
