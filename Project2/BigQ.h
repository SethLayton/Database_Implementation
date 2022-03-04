#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

typedef struct {
	Pipe *inpipe;
	Pipe *outpipe;
	OrderMaker *order;
	int runlength;
	void *context;
}bigqutil;

class BigQ {
	private:
		int currPage = 0;
		File myFile;
		off_t file_length;
		Page myPage;		
		static OrderMaker sortorder;
		int pageCount = 0;
		pthread_t threads = pthread_t();
		Pipe & in;
		Pipe & out;
		OrderMaker so;
		int runlength;
				
	public:
		BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
		~BigQ ();
		void pthreadwait ();
		void *DoWork();
		void FinalSort();
		pthread_t& getpt() {return threads;}

	class Compare {
		private:
			OrderMaker *so;
		public:
			Compare(OrderMaker *sort){
				so = sort;
			}
			Compare() {
				
			}
			bool operator() (Record r1, Record r2) {
				ComparisonEngine c;
				int res = c.Compare(&r1, &r2, so);
				if (res < 0) {
				}
				return (res < 0);
			}
	};

	
};


void * ts(void *arg);
#endif
