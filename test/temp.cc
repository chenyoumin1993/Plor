#include <stdint.h>
#include <stdio.h>
#include <math.h>

int main(int argc, char **argv) {
	double t = 0;

	int times = 100;

	if (argc > 1)
		times = atoi(argv[1]);

	for (int i = 0; i < times; ++i) {
		t = t + pow(2, -t);
	}

	printf("%.4f\n", t);

	return 0;
}
