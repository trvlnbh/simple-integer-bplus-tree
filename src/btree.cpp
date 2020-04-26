#define _CRT_SECURE_NO_WARNINGS

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <stack>
#include <queue>

using namespace std;

struct indexEntry {			//		leaf-node entry.
	int key, bid;

	indexEntry() : key(0), bid(0) {}
	indexEntry(int _k, int _b) : key(_k), bid(_b) {}
};

struct dataEntry {			//		non-leaf-node entry.
	int key, value;

	dataEntry() : key(0), value(0) {}
	dataEntry(int _k, int _v) : key(_k), value(_v) {}
};

class BTree {
public:
	//		needed integers.
	int blockSize, rootBID, depth, lastBID, numEntry;
	int leafLowerBound, leafUpperBound, nonleafSplitLower, nonleafSplitUpper;
	const int zeroBID = 0;

	char *fileName;
	fstream mF;
	queue<pair<int, int>> printQ;
	
	BTree(char *fname);
	~BTree();

	void update_header();
	void insert(int key, int val);
	void search(int key, char *fname);
	void search(int startRange, int endRange, char *fname);
	void print(char *fname);
};

BTree::BTree(char *fname) {

	fileName = fname;
	mF.open(fileName, ios::in | ios::out | ios::binary);

	if (!mF.fail()) {		//		read and calculate the required integers.
		mF.read(reinterpret_cast<char *>(&blockSize), sizeof(blockSize));
		mF.read(reinterpret_cast<char *>(&rootBID), sizeof(rootBID));
		mF.read(reinterpret_cast<char *>(&depth), sizeof(depth));
		mF.seekg(0, ios::end);
		lastBID = mF.tellg();
		lastBID = (lastBID - 12) / blockSize;
		numEntry = (blockSize - 4) / 8;

		if (numEntry % 2 == 1) {
			leafLowerBound = (numEntry - 1) / 2;
			nonleafSplitLower = (numEntry + 1) / 2;
		}
		else {
			leafLowerBound = (numEntry - 1) / 2 + 1;
			nonleafSplitLower = numEntry / 2;
		}
		leafUpperBound = numEntry - 1;
		nonleafSplitUpper = numEntry - nonleafSplitLower;
	}
}

BTree::~BTree() {

	mF.close();
}

void BTree::update_header() {

	mF.seekp(4, ios::beg);
	mF.write(reinterpret_cast<const char *>(&rootBID), sizeof(rootBID));
	mF.write(reinterpret_cast<const char *>(&depth), sizeof(depth));
}

void BTree::insert(int key, int val) {

	if (rootBID == 0) {		//		when there is no entry.
		vector<dataEntry> node(numEntry);
		node[0].key = key;
		node[0].value = val;

		rootBID = 1;
		lastBID++;

		mF.seekp(0, ios::end);
		for (int idx = 0; idx < numEntry; idx++) {
			mF.write(reinterpret_cast<const char *>(&node[idx]), sizeof(dataEntry));
		}

		mF.write(reinterpret_cast<const char *>(&zeroBID), sizeof(zeroBID));
	}
	else {
		int nowOffset = 12 + ((rootBID - 1) * blockSize);
		int nowBID = rootBID, newBID = 0, parentBID = 0, nowDepth = 0, createKey, leftBID;
		dataEntry tmpdataEntry;
		indexEntry tmpindexEntry;
		stack<int> pBIDstack;
		pBIDstack.push(0);
		
		while (nowDepth != depth) {		//		find leaf-node.
			vector<indexEntry> nl_node(numEntry);

			mF.seekg(nowOffset, ios::beg);
			mF.read(reinterpret_cast<char *>(&leftBID), sizeof(leftBID));
			for (int e = 0; e < numEntry; e++) {
				mF.read(reinterpret_cast<char *>(&tmpindexEntry), sizeof(indexEntry));
				if (tmpindexEntry.key <= key && tmpindexEntry.key != 0) {
					leftBID = tmpindexEntry.bid;
				}
				else {
					break;
				}
			}
			pBIDstack.push(nowBID);
			nowBID = leftBID;
			nowOffset = 12 + ((nowBID - 1) * blockSize);
			nowDepth++;
		}

		if (nowDepth == depth) {		//		insert into leaf-node.
			vector<dataEntry> node;
			
			int entryCnt = 0;
			bool insertCheck = false;
			mF.seekg(nowOffset, ios::beg);
			for (int e = 0; e < numEntry; e++) {
				mF.read(reinterpret_cast<char *>(&tmpdataEntry), sizeof(dataEntry));
				if (tmpdataEntry.key != 0) {
					entryCnt++;
				}
				if (tmpdataEntry.key > key || tmpdataEntry.key == 0) {
					if (!insertCheck) {
						node.push_back(dataEntry(key, val));
						insertCheck = true;
						entryCnt++;
					}
					node.push_back(tmpdataEntry);
					if (entryCnt == numEntry) {
						break;
					}
				}
				else if (tmpdataEntry.key < key) {
					node.push_back(tmpdataEntry);
				}
			}

			if (entryCnt == numEntry) {		//		leaf-node is full. split leaf-node.
				vector<dataEntry> node1(numEntry);
				vector<dataEntry> node2(numEntry);

				for (int idx = 0; idx < leafLowerBound; idx++) {
					node1[idx] = node[idx];
				}
				for (int idx = leafLowerBound; idx <= leafUpperBound; idx++) {
					node2[idx - leafLowerBound] = node[idx];
				}

				int tmpBID;
				mF.seekp(nowOffset + blockSize - 4, ios::beg);
				mF.read(reinterpret_cast<char *>(&tmpBID), sizeof(tmpBID));

				lastBID++;
				newBID = lastBID;
				mF.seekp(nowOffset, ios::beg);
				for (int idx = 0; idx < numEntry; idx++) {
					mF.write(reinterpret_cast<const char *>(&node1[idx]), sizeof(dataEntry));
				}
				mF.write(reinterpret_cast<const char *>(&newBID), sizeof(newBID));

				mF.seekp(0, ios::end);
				for (int idx = 0; idx < numEntry; idx++) {
					mF.write(reinterpret_cast<const char *>(&node2[idx]), sizeof(dataEntry));
				}
				mF.write(reinterpret_cast<const char *>(&tmpBID), sizeof(tmpBID));

				createKey = node2[0].key;
				bool parentfullCheck = true;

				while (parentfullCheck) {		//		non-leaf-node insert routine.
					parentfullCheck = false;

					parentBID = pBIDstack.top();
					pBIDstack.pop();

					if (parentBID == 0) {		//		create new non-leaf-node. when root is leaf.
						vector<indexEntry> nl_node(numEntry);
						nl_node[0].key = createKey;
						nl_node[0].bid = newBID;

						depth++;
						lastBID++;
						newBID = lastBID;
						rootBID = newBID;
						mF.seekp(0, ios::end);
						mF.write(reinterpret_cast<const char *>(&nowBID), sizeof(nowBID));
						for (int idx = 0; idx < numEntry; idx++) {
							mF.write(reinterpret_cast<const char *>(&nl_node[idx]), sizeof(indexEntry));
						}
					}
					else {		//		insert entry into parent-node.
						vector<indexEntry> nl_node;
						nowOffset = 12 + ((parentBID - 1) * blockSize);

						entryCnt = 0;
						insertCheck = false;
						mF.seekg(nowOffset, ios::beg);
						mF.read(reinterpret_cast<char *>(&leftBID), sizeof(leftBID));
						for (int e = 0; e < numEntry; e++) {
							mF.read(reinterpret_cast<char *>(&tmpindexEntry), sizeof(indexEntry));
							if (tmpindexEntry.key > key) {
								if (!insertCheck) {
									nl_node.push_back(indexEntry(createKey, newBID));
									insertCheck = true;
								}
								entryCnt++;
							}
							else if (tmpindexEntry.key == 0) {
								if (!insertCheck) {
									nl_node.push_back(indexEntry(createKey, newBID));
									insertCheck = true;
								}
							}
							else if (tmpindexEntry.key < key) {
								entryCnt++;
							}
							nl_node.push_back(tmpindexEntry);
						}

						if (entryCnt == numEntry) {		//		non-leaf-node is full. split non-leaf-node.
							if (!insertCheck) {
								nl_node.push_back(indexEntry(createKey, newBID));
							}
							parentfullCheck = true;

							vector<indexEntry> nl_node1(numEntry);
							vector<indexEntry> nl_node2(numEntry);

							for (int idx = 0; idx < nonleafSplitLower; idx++) {
								nl_node1[idx] = nl_node[idx];
							}
							for (int idx = 0; idx < nonleafSplitUpper; idx++) {
								nl_node2[idx] = nl_node[nonleafSplitLower + idx + 1];
							}

							int tmpBID;
							createKey = nl_node[nonleafSplitLower].key;
							tmpBID = nl_node[nonleafSplitLower].bid;

							lastBID++;
							newBID = lastBID;
							nowBID = parentBID;
							mF.seekp(nowOffset, ios::beg);
							mF.write(reinterpret_cast<const char *>(&leftBID), sizeof(leftBID));
							for (int idx = 0; idx < numEntry; idx++) {
								mF.write(reinterpret_cast<const char *>(&nl_node1[idx]), sizeof(indexEntry));
							}

							mF.seekp(0, ios::end);
							mF.write(reinterpret_cast<const char *>(&tmpBID), sizeof(tmpBID));
							for (int idx = 0; idx < numEntry; idx++) {
								mF.write(reinterpret_cast<const char *>(&nl_node2[idx]), sizeof(indexEntry));
							}
						}

						if (!parentfullCheck) {
							mF.seekp(nowOffset + 4, ios::beg);
							for (int idx = 0; idx < numEntry; idx++) {
								mF.write(reinterpret_cast<const char *>(&nl_node[idx]), sizeof(indexEntry));
							}
						}
					}
				}
			}
			else {
				mF.seekp(nowOffset, ios::beg);
				for (int idx = 0; idx < numEntry; idx++) {
					mF.write(reinterpret_cast<const char *>(&node[idx]), sizeof(dataEntry));
				}
			}
		}
	}

	update_header();
}

void BTree::search(int key, char *fname) {

	int nowOffset = 12 + ((rootBID - 1) * blockSize);
	int nowBID = rootBID, nowDepth = 0, leftBID, ID;
	dataEntry tmpdataEntry;
	indexEntry tmpindexEntry;

	while (nowDepth != depth) {		//		find leaf-node.
		vector<indexEntry> nl_node(numEntry);

		mF.seekg(nowOffset, ios::beg);
		mF.read(reinterpret_cast<char *>(&leftBID), sizeof(leftBID));
		for (int e = 0; e < numEntry; e++) {
			mF.read(reinterpret_cast<char *>(&tmpindexEntry), sizeof(indexEntry));
			if (tmpindexEntry.key <= key && tmpindexEntry.key != 0) {
				leftBID = tmpindexEntry.bid;
			}
			else {
				break;
			}
		}
		nowBID = leftBID;
		nowOffset = 12 + ((nowBID - 1) * blockSize);
		nowDepth++;
	}

	mF.seekg(nowOffset, ios::beg);
	for (int e = 0; e < numEntry; e++) {
		mF.read(reinterpret_cast<char *>(&tmpdataEntry), sizeof(dataEntry));
		if (tmpdataEntry.key == key) {
			ID = tmpdataEntry.value;
			break;
		}
	}

	ofstream writetxtF;
	writetxtF.open(fname, ios::app);
	writetxtF << key << "," << ID << "\n";
	writetxtF.close();
}

void BTree::search(int startRange, int endRange, char *fname) {

	int nowOffset = 12 + ((rootBID - 1) * blockSize);
	int nowBID = rootBID, nowDepth = 0, leftBID;
	dataEntry tmpdataEntry;
	indexEntry tmpindexEntry;

	while (nowDepth != depth) {		//		find leaf-node.
		vector<indexEntry> nl_node(numEntry);

		mF.seekg(nowOffset, ios::beg);
		mF.read(reinterpret_cast<char *>(&leftBID), sizeof(leftBID));
		for (int e = 0; e < numEntry; e++) {
			mF.read(reinterpret_cast<char *>(&tmpindexEntry), sizeof(indexEntry));
			if (tmpindexEntry.key <= startRange && tmpindexEntry.key != 0) {
				leftBID = tmpindexEntry.bid;
			}
			else {
				break;
			}
		}
		nowBID = leftBID;
		nowOffset = 12 + ((nowBID - 1) * blockSize);
		nowDepth++;
	}

	ofstream writetxtF;
	writetxtF.open(fname, ios::app);
	bool rangeCheck = false;
	while (1) {
		mF.seekg(nowOffset, ios::beg);
		for (int e = 0; e < numEntry; e++) {
			mF.read(reinterpret_cast<char *>(&tmpdataEntry), sizeof(dataEntry));
			if (tmpdataEntry.key == 0) {
				break;
			}
			else if (tmpdataEntry.key <= endRange) {
				if (tmpdataEntry.key >= startRange) {
					writetxtF << tmpdataEntry.key << "," << tmpdataEntry.value << "\t";
				}
			}
			else {
				rangeCheck = true;
				break;
			}
		}

		if (rangeCheck) {
			break;
		}
		else {
			mF.seekg(nowOffset + blockSize - 4, ios::beg);
			mF.read(reinterpret_cast<char *>(&nowBID), sizeof(nowBID));
			if (nowBID == 0) {
				break;
			}
			nowOffset = 12 + ((nowBID - 1) * blockSize);
		}
	}

	writetxtF << "\n";
	writetxtF.close();
}

void BTree::print(char *fname) {

	int nowOffset, nowBID, nowDepth, printDepth = 0, leftBID;
	indexEntry tmpindexEntry;
	printQ.push({ rootBID, 0 });

	ofstream writetxtF;
	writetxtF.open(fname, ios::app);
	while (!printQ.empty()) {		//		BFS traverse
		nowBID = printQ.front().first;
		nowDepth = printQ.front().second;
		nowOffset = 12 + ((nowBID - 1) * blockSize);
		printQ.pop();

		if (printDepth == nowDepth) {
			writetxtF << "<" << printDepth << ">\n";
			printDepth++;
		}

		mF.seekg(nowOffset, ios::beg);
		if (nowDepth != depth) {
			mF.read(reinterpret_cast<char *>(&leftBID), sizeof(leftBID));
			if (nowDepth == 0 && depth != 0) {
				printQ.push({ leftBID, nowDepth + 1 });
			}
		}
		for (int e = 0; e < numEntry; e++) {
			mF.read(reinterpret_cast<char *>(&tmpindexEntry), sizeof(indexEntry));
			if (tmpindexEntry.bid == 0) {
				if (printQ.empty() || printQ.front().second == nowDepth + 1) {
					writetxtF << "\n";
				}
				else {
					writetxtF << ",";
				}
				break;
			}
			if (e != 0) {
				writetxtF << ",";
			}
			writetxtF << tmpindexEntry.key;
			
			if (nowDepth == 0 && depth != 0) {
				printQ.push({ tmpindexEntry.bid, nowDepth + 1 });
			}
		}
	}

	writetxtF.close();
}

int main(int argc, char* argv[]) {

	ofstream createoutF;
	ifstream readtxtF;
	char command = argv[1][0];
	char *binName = argv[2];
	char *readtxtFile, *outputtxtName;
	char lineBuffer[22];
	
	BTree *BT = new BTree(binName);

	switch (command) {
	case 'c':
		//		create file header.
		int fileHeader[3];
		createoutF.open(binName, ios::binary);

		fileHeader[0] = atoi(argv[3]);
		fileHeader[1] = 0;
		fileHeader[2] = 0;
		createoutF.write(reinterpret_cast<const char *>(&fileHeader), sizeof(fileHeader));
		createoutF.close();

		break;
	case 'i':
		//		insert
		readtxtFile = argv[3];
		readtxtF.open(readtxtFile);

		if (!readtxtF.fail()) {
			while (readtxtF.getline(lineBuffer, sizeof(lineBuffer))) {
				char *insertTmp = strtok(lineBuffer, ",");
				int insertKey = atoi(insertTmp);
				insertTmp = strtok(NULL, " ");
				int insertVal = atoi(insertTmp);

				BT->insert(insertKey, insertVal);
			}
		}

		break;
	case 's':
		//		look-up
		readtxtFile = argv[3];
		outputtxtName = argv[4];
		readtxtF.open(readtxtFile);

		if (!readtxtF.fail()) {
			while (readtxtF.getline(lineBuffer, sizeof(lineBuffer))) {
				int searchKey = atoi(lineBuffer);
				if (searchKey == 0) {
					continue;
				}

				BT->search(searchKey, outputtxtName);
			}
		}

		break;
	case 'r':
		//		range search
		readtxtFile = argv[3];
		outputtxtName = argv[4];
		readtxtF.open(readtxtFile);

		if (!readtxtF.fail()) {
			while (readtxtF.getline(lineBuffer, sizeof(lineBuffer))) {
				char *insertTmp = strtok(lineBuffer, ",");
				int rangeLeft = atoi(insertTmp);
				insertTmp = strtok(NULL, " ");
				int rangeRight = atoi(insertTmp);

				BT->search(rangeLeft, rangeRight, outputtxtName);
			}
		}

		break;
	case 'p':
		//		print
		outputtxtName = argv[3];
		BT->print(outputtxtName);

		break;
	}

	delete BT;
	return 0;
}