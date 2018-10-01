#include "btree.h"


int main(int argc, char const *argv[])
{
	BTree* config = makeBTree(5, 1, 1);
	config->root->keys[0] = 1;
	config->root->keys[1] = 5;
	config->root->keys[2] = 7;
	config->root->keys[3] = 9;
	return 0;
}
