/*
 * Copyright (c) 2024, Arka Mondal. All rights reserved.
 * Use of this source code is governed by a BSD-style license that
 * can be found in the LICENSE file.
 */

#ifndef __MAPREDUCE_H__
#define __MAPREDUCE_H__

typedef char *(*getter)(char *key, unsigned long partition_number);
typedef void (*mapper)(char *file_name);
typedef void (*reducer)(char *key, getter get_func, unsigned long partition_number);
typedef unsigned long (*partitioner)(char *key, unsigned int num_partitions);

void mr_emit(char *key, char *value);
unsigned long mr_default_hashpartition(char *key, unsigned int num_partitions);
void mr_run(int argc, char *argv[], mapper map, unsigned int num_mappers,
            reducer reduce, unsigned int num_reducers,
            partitioner partition);

#endif /* __MAPREDUCE_H__ */
