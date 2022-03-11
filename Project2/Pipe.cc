#include "Pipe.h"

#include <iostream>
#include <stdlib.h> 

Pipe :: Pipe (int bufferSize, std::string n) {
	name = n;
	// set up the mutex assoicated with the pipe
	pthread_mutex_init (&pipeMutex, NULL);

	// set up the condition variables associated with the pipe
	pthread_cond_init (&producerVar, NULL);
	pthread_cond_init (&consumerVar, NULL);

	// set up the pipe's buffer
	buffered = new (std::nothrow) Record[bufferSize];
	if (buffered == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
	pthread_mutex_lock (&pipeMutex);
	totSpace = bufferSize;
	firstSlot = lastSlot = 0;
	// note that the pipe has not yet been turned off
	done = 0;
	pthread_mutex_unlock (&pipeMutex);
}

Pipe :: ~Pipe () {

	// free everything up!
	delete [] buffered;

	pthread_mutex_destroy (&pipeMutex);
	pthread_cond_destroy (&producerVar);
	pthread_cond_destroy (&consumerVar);
	
}


void Pipe :: Insert (Record *insertMe) {
	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);
	// next, see if there is space in the pipe for more data; if
	// there is, then do the insertion
	if (lastSlot - firstSlot < totSpace) {
		buffered [lastSlot % totSpace].Consume (insertMe);
	// if there is not, then we need to wait until the consumer
	// frees up some space in the pipeline
	} else {
		pthread_cond_wait (&producerVar, &pipeMutex);
		buffered [lastSlot % totSpace].Consume (insertMe);
	}
	
	// note that we have added a new record
	lastSlot++;

	// done!
	pthread_mutex_unlock (&pipeMutex);

	// signal the consumer who might now want to suck up the new
	// record that has been added to the pipeline
	pthread_cond_broadcast (&consumerVar);
	
	 
}

int Pipe :: Remove (Record *removeMe) {
	// cout << "Pipe::Remove() - get mutex lock" << endl; 
	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);
	// next, see if there is anything in the pipeline; if
	// there is, then do the removal
	if (lastSlot != firstSlot) {
		//cout << "Pipe::Remove() - Consume" << endl; 
		removeMe->Consume (&buffered [firstSlot % totSpace]);
		//cout << "Pipe::Remove() - Finish Consume" << endl; 
	// if there is not, then we need to wait until the producer
	// puts some data into the pipeline
	} else {
		// the pipeline is empty so we first see if this
		// is because it was turned off
		if (done) {
			//cout << "Pipe::Remove() - Unlock mutex 1" << endl; 
			pthread_mutex_unlock (&pipeMutex);
			//cout << "Pipe::Remove() - Unlock done 1" << endl;
			return 0;
		}

		// wait until there is something there
		//cout << "Pipe::Remove() - WAIT" << endl; 
		int r = pthread_cond_wait (&consumerVar, &pipeMutex);
		// since the producer may have decided to turn off
		// the pipe, we need to check if it is still open
		if (done && lastSlot == firstSlot) {
			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}
		removeMe->Consume (&buffered [firstSlot % totSpace]);
	}
	
	// note that we have deleted a record
	firstSlot++;
	//cout << "Pipe::Remove() - UNLOCK 2" << endl; 
	pthread_mutex_unlock (&pipeMutex);

	// signal the producer who might now want to take the slot
	// that has been freed up by the deletion
	//cout << "Pipe::Remove() - BROADCAST" << endl; 
	pthread_cond_broadcast (&producerVar);
	// cout << "Pipe::Remove() - BROADCAST DONE" << endl;
	// done!
	
	return 1;
}

void Pipe :: ShutDown () {

	// first, get a mutex on the pipeline
        pthread_mutex_lock (&pipeMutex);

	// note that we are now done with the pipeline
	done = 1;

	// signal the consumer who may be waiting
	pthread_cond_signal (&consumerVar);

	// unlock the mutex
	pthread_mutex_unlock (&pipeMutex);
	
}
