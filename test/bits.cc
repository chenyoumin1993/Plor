#include <stdio.h>
#include <stdint.h>

#define TEMP 1 

int main()
{
	uint64_t LOCK_BIT = (1ULL << 63);
	uint64_t lt = 0;

	// lt = lt | LOCK_BIT;
	printf("%llx\n", lt);

	lt = lt | (1ULL << 8);
	printf("%llx\n", lt);

	lt = lt & (~LOCK_BIT);
	printf("%llx\n", lt);

	printf("%llu\n", sizeof(TEMP));

	return 0;
}
