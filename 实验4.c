/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include "mpi.h"
#include <string.h>
#include <math.h>
inline double f(double x) {  
    return 4/(1+x*x);  
}   
int main (int argc, char *argv[] )
{
    double pi, h, sum, x, startime, endtime;  
    int size, rank;  
    int i,n;  
    MPI_Init(0,0);  
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);  
    MPI_Comm_size(MPI_COMM_WORLD, &size); 
    if(rank == 0) {  
        printf( "Please enter n\n" );  
        scanf("%d",&n);  
        startime = MPI_Wtime();  
    }  
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);  //将n广播到所有进程中  
    h = 1.0/n;                             //每个矩形块的宽  
    sum = 0.0;          
    for( i=rank; i<n; i+=size) {               
        x = h*((double)i - 0.5);  
        sum = sum + f(x);  
    }            
    sum = sum * h;                                 //每个进程计算的矩形面积之和  
    MPI_Reduce(&sum, &pi, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);  //利用归约操作(MPI_SUM)将所有进程的sum累加到root进程(0)的sum当中得到结果  
    if(rank == 0) {  
        endtime = MPI_Wtime();  
        printf("用时:%f\n", endtime-startime);  
        printf("%0.15f\n", pi);  
    }

    MPI_Finalize();
    return 0; 
}
