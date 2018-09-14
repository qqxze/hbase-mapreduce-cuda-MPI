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
    int int_data=99;
    float float_data=9.9;
    int postion = 0; 
    MPI_Status status;
    if(rank==0){
	 
	 MPI_Pack(&int_data,1,MPI_INT,greeting,MAX_STRING,&postion,MPI_COMM_WORLD);
         MPI_Pack(&float_data,1,MPI_FLOAT,greeting,MAX_STRING,&postion,MPI_COMM_WORLD);
	 MPI_Bcast(&greeting,MAX_STRING,MPI_CHAR,0,MPI_COMM_WORLD);
         printf(" Process %d of %d on %s is MPI_Bcast. Send INT number is %d , and FLOAT number is %f.\n",rank,size,processor_name,int_data,float_data);
    }
    else{
         MPI_Bcast(greeting,MAX_STRING,MPI_CHAR,0,MPI_COMM_WORLD);
         MPI_Unpack(greeting,MAX_STRING,&postion,&int_data,1,MPI_INT,MPI_COMM_WORLD);
	 MPI_Unpack(greeting,MAX_STRING,&postion,&float_data,1,MPI_FLOAT,MPI_COMM_WORLD);
         printf(" Process %d of %d on %s receive the Bcast. Receive INT number is %d , and FLOAT number is %f.\n",rank,size,processor_name,int_data,float_data);

    }
    MPI_Finalize();
    return 0;
}
