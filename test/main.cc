#include <stdio.h>
#include <stdint.h>

struct Node {
	int key;
	Node *next;
};

Node *head = NULL;

bool insert(Node *n) {
	Node *cur, *prev;
_start:
	cur = head;
	prev = NULL;
	n->next = NULL;
	if (__sync_bool_compare_and_swap((int *)head, NULL, (uint64_t)n)) 
		return true;
	
	while (cur != NULL) {
		if (cur->key == n->key) {
			// Don't allow insert the already existing node.
			printf("err.\n");
			return false;
		} else if (cur->key < n->key) {
			// Move to the next one.
			prev = cur;
			cur = cur->next;
		} else {
			// I should be inserted behind cur.
			n->next = cur;
			if (prev != NULL) {
				// I'm in the waiting list.
				if (!__sync_bool_compare_and_swap((int *)&(prev->next), (uint64_t)cur, (uint64_t)n)) {
					// Can fail, others has already inserted between in, retry.
					goto _start;
				} else {
					// Inserted in some pos. success.
					if (prev->deleted && prev->next == n) {
						// Prev is deleted, retry.
						goto _start;
					}
					return true;
				}
			} else {
				goto _start;
			}
		}
	}
	goto _start;
}

int main() {
	return 0;
}
