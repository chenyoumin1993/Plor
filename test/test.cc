#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

int main()
{
	uint64_t a = 16;
	a |= 1ull;
	printf("%d\n", a);
}
