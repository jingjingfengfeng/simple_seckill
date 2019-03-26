
#include <stdio.h>
using namespace std;

enum LOG_LEVEL {
	LOG_DEBUG = 0,
	LOG_NOTICE = 1,
	LOG_ERROR  = 2
};

#define LogWater(iLevel, sWater, args...) do {\
	printf(sWater,## args);printf("\n");} while(0)
