#include <stdio.h>
#include <stdint.h>
#include <thread>

#define MAX 10000

struct Node {
	int key;
	union {
		struct {
			uint64_t deleted : 1;
			uint64_t _next : 63;
		};
		Node *next;
	};
};

inline bool is_mark_set(uint64_t addr) {
	return (addr & 0x1ull) == 1;
}

inline void set_mark(uint64_t *addr) {
	*addr |= 0x1ull;
}

inline void unset_mark(uint64_t *addr) {
	if (is_mark_set(*addr))
		*addr -= 0x1ull;
}


Node *head = NULL;

bool insert(Node *n) {
	Node *cur, *prev;
	// Insert myself to the head.
_start:
	// Read a snapshot
	uint64_t h = (uint64_t)head;
	if ((Node *)h == NULL) {
		n->next = NULL;
		if (!__sync_bool_compare_and_swap(&head, (uint64_t)NULL, (uint64_t)n))
			goto _start;
	} else {
		// Read a snapshot of head->next.
		n->next = (Node *)h;
		if (!__sync_bool_compare_and_swap(&head, (uint64_t)h, (uint64_t)n)) {
			goto _start;
		}
	}
}

bool remove(int key) {
		Node *cur, *prev;
_start:
	cur = head;
	prev = NULL;
	// Find myself
	while (cur != NULL) {
		uint64_t next = (uint64_t)cur->next;
		if (is_mark_set(next))
			goto _start;
		if (cur->key == key)
			break;
		prev = cur;
		cur = (Node *)next;
	}
	if (cur == NULL) {
		// cannot find myself.
		return false;
	} else {
		// Set myself to deleted.
		uint64_t t1, t2;
		t1 = t2 = (uint64_t)cur->next;
		set_mark(&t2);
		if (!__sync_bool_compare_and_swap(&cur->next, (uint64_t)t1, (uint64_t)t2)) 
			goto _start;
		// asm volatile ("mfence" ::: "memory");
		if (prev == NULL) {
			// I'm the head (i.e., the owner)
			if (!__sync_bool_compare_and_swap(&head, (uint64_t)cur, (uint64_t)t1)) {
				unset_mark((uint64_t *)&cur->next);
				goto _start;
			}
		} else {
			uint64_t t3 = (uint64_t)cur->next;
			unset_mark(&t3);
			if (!__sync_bool_compare_and_swap(&prev->next, (uint64_t)cur, (uint64_t)t3)) {
				unset_mark((uint64_t *)&cur->next);
				goto _start;
			}
		}
	}
}

void worker1(int id, Node *t) {
	for (int i = 0; i < MAX; i++) {
		t[i].key = id * MAX + i;
		insert(&t[i]);
	}
}

void worker2(int id, Node *t) {
	for (int i = 0; i < MAX; i++) {
		remove(id * MAX + i);
	}
}

int main(int argc, char **argv) {
	int n_thd = 1;
	if (argc > 1) n_thd = atoi(argv[1]);

	Node *n = (Node *)malloc(sizeof(Node) * MAX * n_thd);
	
	std::thread *t = new std::thread[n_thd];

	for (int i = 0; i < n_thd; ++i) {
		t[i] = std::thread(worker1, i, &n[i * MAX]);
	}

	for (int i = 0; i < n_thd; ++i)
		t[i].join();
	
	Node *cur = head;
	int cnt = 0;
	while (cur != NULL) {
		cur = cur->next;
		cnt += 1;
	}

	printf("%d\n", cnt);

	for (int i = 0; i < n_thd; ++i) {
		t[i] = std::thread(worker2, i, &n[i * MAX]);
	}

	for (int i = 0; i < n_thd; ++i)
		t[i].join();

	cur = head;
	cnt = 0;
	while (cur != NULL) {
		cur = cur->next;
		cnt += 1;
	}

	printf("%d\n", cnt);
	
	for (int i = 0; i < MAX * n_thd; i++) {
		if (n[i].key != i || n[i].deleted != 0x1ull) {
			printf("%d\n", i);
		}
	}
	free(n);
	delete[] t;
	return 0;
}
