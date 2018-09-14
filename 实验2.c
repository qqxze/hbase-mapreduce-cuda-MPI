/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include "mpi.h"
#include <string.h>
const int MAX_STRING=100;
int main( int argc, char *argv[] )
{
    int rank;
    int size;
    int namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    char greeting[MAX_STRING];
    MPI_Init( 0, 0 );
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(processor_name,&namelen);
    if(rank!=0){
      sprintf(greeting,"Hello World! Process %d of %d on %s", rank, size,processor_name);
      MPI_Send(greeting, strlen(greeting)+1,MPI_CHAR,0,0,MPI_COMM_WORLD);
    }
    else{
   	 printf("Hello World! Process %d of %d on %s\n", rank, size,processor_name);
   	 for(int q=1;q<size;q++){
       		MPI_Recv( greeting, MAX_STRING,MPI_CHAR,q,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      		printf("%s\n",greeting);
    	 }
   	 printf("Process on %s has executed over!\n",processor_name);
    }
    MPI_Finalize();
    return 0;
}
