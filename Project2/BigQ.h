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
		long pageCount = 0;		
	public:
		BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
		~BigQ ();
		void *DoWork(void *arg);
		void FinalSort(bigqutil *b);


	class Compare {
		private:
			OrderMaker* so;
		public:
			Compare(OrderMaker *sort){
				so = sort;
			}
			Compare() {}
			bool operator() (Record r1, Record r2) {
				Schema ms("catalog", "region");
				//cout << "comparing records:" << endl;
				//r1.Print(&ms);
				//r2.Print(&ms);
				ComparisonEngine c;
				int res = c.Compare(&r1, &r2, so);
				if (res <= 0) {
					//cout << "first record is smaller" << endl;
				}
				return (res <= 0);
			}
	};

	
};


void * ts(void *arg);
#endif
