#include <stdio.h>
#include "rdkafka.h"
#include <string.h>
#include <malloc.h>

static long int msgs_wait_cnt = 0;
static long int msgs_wait_produce_cnt = 0;
static int forever = 1;

static struct {
	uint64_t msgs;
	uint64_t bytes;
	uint64_t msgs_dr_err;
	uint64_t msgs_dr_ok;
	uint64_t bytes_dr_ok;
	uint64_t bytes_dr_err;
} cnt;

static void msg_delivered(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	msgs_wait_cnt--;

	if(rkmessage->err)
	{
		cnt.msgs_dr_err++;
		cnt.bytes_dr_err += rkmessage->len;
	}
	else
	{
		cnt.msgs_dr_ok++;
		cnt.bytes_dr_ok += rkmessage->len;
	}

	//printf("%%Messages wait:[%ld]\n",msgs_wait_cnt);
	if(msgs_wait_produce_cnt == 0 && msgs_wait_cnt == 0 && !forever)
	{
		printf("%% Total %llu messages of size %llu is delivered!\n"
				"%% %llu messages of size %llu is produced\n"
				"%% %llu messages of size %llu is failed to produce!\n",
				cnt.msgs,
				cnt.bytes,
				cnt.msgs_dr_ok,
				cnt.bytes_dr_ok,
				cnt.msgs_dr_err,
				cnt.bytes_dr_err
				);		
	}
}

int main(int argc, char **argv)
{
	const char *brokers;
	const char *topic;
	char errstr[512];
	int msgcnt;
	int msgsize = 10000;
	int start;
	int end; 

	brokers = "172.37.37.14";
	topic   = "test";

	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;

	conf = rd_kafka_conf_new();
	rd_kafka_conf_set(conf,"queue.buffering.max.messages","500000",NULL,0);
	rd_kafka_conf_set(conf,"messages.send.max.retries","3",NULL,0);
	rd_kafka_conf_set(conf,"retry.backoff.ms","500",NULL,0);
	rd_kafka_conf_set(conf,"bootstrap.servers",brokers,errstr,sizeof(errstr));
	rd_kafka_conf_set( conf, "compression.codec", "gzip", errstr, sizeof(errstr));

	rd_kafka_conf_set_dr_msg_cb(conf,msg_delivered);

	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if(!rk)
	{
		printf("Failed to create new producer: %s",errstr);
	}	

	rkt = rd_kafka_topic_new(rk, topic, NULL);
	if(!rkt)
	{
		printf("Failed to create topic object:%s\n",errstr);
	}

	FILE *fp;
	fp = fopen("test_kafka.db","r");
	fseek(fp, 0L, SEEK_END);
	size_t flen = ftell(fp);

	if((flen % msgsize) == 0)
	{
		msgcnt = (int)(flen / msgsize);
		printf("%%msgcnt:%d\n", msgcnt);
	}
	else
	{
		msgcnt = (int)(flen / msgsize) + 1;
		printf("%%msgcnt:%d\n",msgcnt);
	}
	

	if(msgcnt ==  -1)
	{
		forever = 0;
	}	

	msgs_wait_produce_cnt = msgcnt;

	start = 0;
	end   = flen;
	char *p = (char *)malloc(msgsize);
	fseek(fp, 0, SEEK_SET);

	while(start < end)
	{	
		memset(p, 0, msgsize);
		int xlen = (end-start) >= msgsize ? msgsize : (end-start);
		int val = fread(p, xlen, 1, fp);		
		printf("%d + %d: %d\n",start, xlen, val);
		start += xlen;					

		while (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,p,xlen,NULL,0,NULL) == -1)
		{
			printf("Failed to produce to topic %s: %s\n",rd_kafka_topic_name(rkt),rd_kafka_err2str(rd_kafka_last_error()));

			if(rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) // QUERY IS FULL
			{
				rd_kafka_poll(rk, 1000);
			}
		}

		msgs_wait_cnt++;
		

		if(msgs_wait_produce_cnt != -1)
		{
			msgs_wait_produce_cnt--;
			//printf("%%Messages wait to produce:[%ld]\n", msgs_wait_produce_cnt);
		}

		cnt.msgs++;
		cnt.bytes += xlen;
		rd_kafka_poll(rk,0);
	}
	
	printf("Topic:[%s]\n",rd_kafka_topic_name(rkt));
	forever = 0;
	printf("Flush final messages...\n");	
	rd_kafka_flush(rk,1000);
	rd_kafka_topic_destroy(rkt);
	rd_kafka_destroy(rk);

	fclose(fp);
	if(p)
	{
		free(p);
	}
	return 0;
}