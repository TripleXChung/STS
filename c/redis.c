#include <sys/types.h>
#include <sys/socket.h>

#include "redis.h"

struct redis_client *redis_connect(char *host, short int port)
{
	struct redis_client *client;

	client = malloc(sizeof(struct redis_client));
	if(NULL == client) {
		return NULL;
	}

	client->fd = socket(PF_INET, SOCK_STREAM, 0);
	return client;
err:
	free(client);
	return NULL;
}

int redis_publish(struct redis_client *client, char *channel, char *message)
{

}

int redis_subscribe(struct redis_client *client, char *pattern, redis_callback_t *callback, void *context)
{

}
