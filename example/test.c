/*
 * Copyright (c) 2024, Arka Mondal. All rights reserved.
 * Use of this source code is governed by a BSD-style license that
 * can be found in the LICENSE file.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mapreduce.h"

static void map(char *filename)
{
  char *line, *token, *temp;
  size_t size;
  FILE *fp;

  line = NULL;
  size = 0;

  fp = fopen(filename, "r");
  if (fp == NULL)
    exit(EXIT_FAILURE);

  while (getline(&line, &size, fp) != -1)
  {
    temp = line;
    token = temp;
    while ((token = strsep(&temp, " \t\n\r")) != NULL)
    {
      if (token[0] != '\0')
        mr_emit(token, "1");
    }
  }

  free(line);
  fclose(fp);
}

static void reduce(char *key, getter get_next, unsigned long partition_number)
{
  char *value;
  int count;

  count = 0;

  while ((value = get_next(key, partition_number)) != NULL)
    count++;

  printf("%s: %d\n", key, count);
}

int main(int argc, char *argv[])
{
  mr_run(argc, argv, map, 2, reduce, 2, mr_default_hashpartition);
  return 0;
}
