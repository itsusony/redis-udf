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
#define UNIXSOCKET "/tmp/redis.sock"

my_bool redis_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {

    redisContext *rdsCon;

    if(args->arg_count != 1){
        strcpy(message, "redis() requires one argument");
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
    redisContext* rdsCon;
    redisReply *rdsRply;
    char* command;
    unsigned long cmdLen;

    result[0] = '\0';
    *length = 0;

    // Connection
    rdsCon = (redisContext*)initid->ptr;

    // Create Null-terminated string
    cmdLen = args->lengths[0];
    if(cmdLen==0){
        *is_null=1;
        return result;
    }
    command = strdup(args->args[0]);

    // Exec command
    rdsRply = redisCommand(rdsCon, command);
    free(command);

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

    freeReplyObject(rdsRply);
    return result;
}
