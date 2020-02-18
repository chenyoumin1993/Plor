#include <stdio.h>
#include <string>

using namespace std;

int foo(int &a) {
	a = 1;
	printf("%d\n", a);
}

int main() {
	string a = "dsfsd";
	// string b = move(a);
	string b = a;
}
