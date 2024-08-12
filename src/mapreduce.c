/*
 * Copyright (c) 2024, Arka Mondal. All rights reserved.
 * Use of this source code is governed by a BSD-style license that
 * can be found in the LICENSE file.
 */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"

struct mr_vnode {
  struct mr_vnode *prev;
  struct mr_vnode *next;
  char *value;
};

struct mr_kvnode {
  struct mr_kvnode *prev;
  struct mr_kvnode *next;
  char *key;
  struct mr_vnode *start;
  struct mr_vnode *access_next;
};

struct mr_master_kvnode {
  pthread_mutex_t mtx;
  struct mr_kvnode *start;
};

struct mapper_dispatch_packet {
  char **arglist;
  mapper mapper_fn;
  unsigned int this_id;
  unsigned int step_count;
  unsigned int iternation;
  unsigned int total_item_count;
};

struct reducer_dispatch_packet {
  reducer reducer_fn;
  unsigned int this_id;
};

static struct mr_master_kvnode *master_kvlist;
static partitioner partition_fn;
static unsigned int num_partitions;

static int mr_master_kvlist_init(void);
static int mr_master_kvlist_destroy(void);

static struct mr_kvnode * mr_kvnode_new(char *key);
static struct mr_vnode * mr_vnode_new(char *value);

static void mr_kvlist_push(char *key, char *value, unsigned long partition_number);
static void mr_vlist_push(struct mr_kvnode *kvnode, char *value);

static void mr_kvlist_destroy(struct mr_kvnode *start);
static void mr_vlist_destroy(struct mr_vnode *start);

static void * mapper_dispatcher(void *arg);
static char * mr_get_next_value(char *key, unsigned long partition_number);

static int mr_master_kvlist_init(void)
{
  unsigned int i;

  master_kvlist = malloc(num_partitions * sizeof(struct mr_master_kvnode));
  if (master_kvlist == NULL)
    return -1;

  for (i = 0; i < num_partitions; i++)
  {
    master_kvlist[i].start = NULL;
    if (pthread_mutex_init(&master_kvlist[i].mtx, NULL) != 0)
      return -1;
  }

  return 0;
}

static int mr_master_kvlist_destroy(void)
{
  unsigned int i;


  for (i = 0; i < num_partitions; i++)
  {

    pthread_mutex_lock(&master_kvlist[i].mtx);
    mr_kvlist_destroy(master_kvlist[i].start);
    pthread_mutex_unlock(&master_kvlist[i].mtx);

    if (pthread_mutex_destroy(&master_kvlist[i].mtx) != 0)
      return -1;
  }

  free(master_kvlist);

  return 0;
}

static struct mr_kvnode * mr_kvnode_new(char *key)
{
  struct mr_kvnode *temp;

  temp = malloc(sizeof(struct mr_kvnode) + ((strlen(key) + 1) * sizeof(char)));
  if (temp == NULL)
    return NULL;

  temp->next = NULL;
  temp->prev = NULL;

  temp->start = NULL;
  temp->access_next = NULL;

  temp->key = (char *) (temp + 1);
  strcpy(temp->key, key);

  return temp;
}

static struct mr_vnode * mr_vnode_new(char *value)
{
  struct mr_vnode *temp;

  temp = malloc(sizeof(struct mr_vnode) + ((strlen(value) + 1) * sizeof(char)));
  if (temp == NULL)
    return NULL;

  temp->next = NULL;
  temp->prev = NULL;

  temp->value = (char *) (temp + 1);
  strcpy(temp->value, value);

  return temp;
}

static void mr_kvlist_push(char *key, char *value, unsigned long partition_number)
{
  struct mr_kvnode *itr, *temp;

  if (master_kvlist[partition_number].start == NULL)
  {
    temp = mr_kvnode_new(key);
    if (temp == NULL)
      exit(EXIT_FAILURE);

    master_kvlist[partition_number].start = temp;
  }
  else
  {
    for (itr = master_kvlist[partition_number].start; itr != NULL; itr = itr->next)
    {
      if (strcmp(itr->key, key) > 0)
      {
        temp = mr_kvnode_new(key);
        if (temp == NULL)
          exit(EXIT_FAILURE);

        temp->prev = itr->prev;
        temp->next = itr;
        itr->prev = temp;

        if (temp->prev != NULL)
          temp->prev->next = temp;
        else
          master_kvlist[partition_number].start = temp;

        break;
      }
      else if (strcmp(itr->key, key) == 0)
      {
        temp = itr;
        break;
      }

      if (itr->next == NULL)
      {
        temp = mr_kvnode_new(key);
        if (temp == NULL)
          exit(EXIT_FAILURE);

        itr->next = temp;
        temp->prev = itr;
        temp->next = NULL;
        break;
      }
    }
  }

  mr_vlist_push(temp, value);
  temp->access_next = temp->start;

}

static void mr_vlist_push(struct mr_kvnode *kvnode, char *value)
{
  struct mr_vnode *itr, *temp;

  temp = mr_vnode_new(value);
  if (temp == NULL)
    exit(EXIT_FAILURE);

  if (kvnode->start == NULL)
  {
    kvnode->start = temp;
  }
  else
  {
    for (itr = kvnode->start; itr != NULL; itr = itr->next)
    {
      if (strcmp(itr->value, value) > 0)
      {
        temp->prev = itr->prev;
        temp->next = itr;
        itr->prev = temp;

        if (temp->prev != NULL)
          temp->prev->next = temp;
        else
          kvnode->start = temp;

        break;
      }

      if (itr->next == NULL)
      {
        itr->next = temp;
        temp->prev = itr;
        temp->next = NULL;
        break;
      }
    }

  }
}

static void mr_kvlist_destroy(struct mr_kvnode *start)
{
  struct mr_kvnode *temp;

  while (start != NULL)
  {
    mr_vlist_destroy(start->start);
    temp = start;
    start = start->next;
    free(temp);
  }
}

static void mr_vlist_destroy(struct mr_vnode *start)
{
  struct mr_vnode *temp;

  while (start != NULL)
  {
    temp = start;
    start = start->next;
    free(temp);
  }
}

static void * mapper_dispatcher(void *arg)
{
  char **arglist;
  unsigned int i, this_id;
  mapper mapper_fn;
  struct mapper_dispatch_packet *packet;

  packet = (struct mapper_dispatch_packet *) arg;
  mapper_fn = packet->mapper_fn;
  arglist = packet->arglist;
  this_id = packet->this_id;

  for (i = 0; i < packet->iternation; i++)
    mapper_fn(arglist[this_id + 1 + (i * packet->step_count)]);

  // remaing files for this specific threads
  // ((packet->total_item_count - (packet->iternation * packet->step_count)) > this_id)
  if (packet->total_item_count > (this_id + (packet->iternation * packet->step_count)))
    mapper_fn(arglist[this_id + 1 + (packet->iternation * packet->step_count)]);

  return NULL;
}

static void * reducer_dispatcher(void *arg)
{
  struct mr_kvnode *itr;
  struct reducer_dispatch_packet *packet;

  packet = (struct reducer_dispatch_packet *) arg;

  for (itr = master_kvlist[packet->this_id].start; itr != NULL; itr = itr->next)
    packet->reducer_fn(itr->key, mr_get_next_value, packet->this_id);

  return NULL;
}

static char * mr_get_next_value(char *key, unsigned long partition_number)
{
  char * value;
  struct mr_kvnode *temp;

  value = NULL;

  // locking not needed as reducers have one-to-one relation with partitions
  for (temp = master_kvlist[partition_number].start; temp != NULL; temp = temp->next)
  {
    if (strcmp(temp->key, key) != 0)
      continue;

    if (temp->access_next != NULL)
    {
      value = temp->access_next->value;
      temp->access_next = temp->access_next->next;
    }
    break;
  }

  return value;
}

unsigned long mr_default_hashpartition(char *key, unsigned int num_partitions)
{
  int ch;
  unsigned long hash;

  hash = 5381;

  while ((ch = *key++) != '\0')
    hash = hash * 33 + ch;

  return hash % num_partitions;
}

void mr_emit(char *key, char *value)
{
  unsigned long partition_number;

  partition_number = partition_fn(key, num_partitions);

  pthread_mutex_lock(&master_kvlist[partition_number].mtx);

  mr_kvlist_push(key, value, partition_number);

  pthread_mutex_unlock(&master_kvlist[partition_number].mtx);
}

void mr_run(int argc, char *argv[], mapper map, unsigned int num_mappers,
            reducer reduce, unsigned int num_reducers,
            partitioner partition)
{
  unsigned int j;
  pthread_t *mappers, *reducers;
  struct mapper_dispatch_packet *mpacketlist;
  struct reducer_dispatch_packet *rpacketlist;

  partition_fn = partition;
  num_partitions = num_reducers;

  if (mr_master_kvlist_init() != 0)
    exit(EXIT_FAILURE);

  mappers = malloc(num_mappers * sizeof(pthread_t));
  if (mappers == NULL)
    goto cleanup;

  mpacketlist = malloc(num_mappers * sizeof(struct mapper_dispatch_packet));
  if (mpacketlist == NULL)
  {
    free(mappers);
    goto cleanup;
  }

  for (j = 0; j < num_mappers; j++)
  {
    mpacketlist[j].arglist = argv;
    mpacketlist[j].mapper_fn = map;
    mpacketlist[j].this_id = j;
    mpacketlist[j].total_item_count = argc - 1;
    mpacketlist[j].step_count = num_mappers;
    mpacketlist[j].iternation = mpacketlist[j].total_item_count / num_mappers;

    if (pthread_create(&mappers[j], NULL, mapper_dispatcher, &mpacketlist[j]) != 0)
      exit(EXIT_FAILURE);
  }

  for (j = 0; j < num_mappers; j++)
    pthread_join(mappers[j], NULL);


  free(mappers);
  free(mpacketlist);

  reducers = malloc(num_reducers * sizeof(pthread_t));
  if (reducers == NULL)
    goto cleanup;

  rpacketlist = malloc(num_reducers * sizeof(struct reducer_dispatch_packet));
  if (rpacketlist == NULL)
  {
    free(reducers);
    goto cleanup;
  }

  for (j = 0; j < num_mappers; j++)
  {
    rpacketlist[j].this_id = j;
    rpacketlist[j].reducer_fn = reduce;

    if (pthread_create(&reducers[j], NULL, reducer_dispatcher, &rpacketlist[j]) != 0)
      exit(EXIT_FAILURE);
  }

  for (j = 0; j < num_mappers; j++)
    pthread_join(reducers[j], NULL);

  free(reducers);
  free(rpacketlist);

cleanup:
  if (mr_master_kvlist_destroy() != 0)
    exit(EXIT_FAILURE);
}
