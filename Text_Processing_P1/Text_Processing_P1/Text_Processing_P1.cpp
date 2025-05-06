#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <cctype>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>

using namespace std;
using namespace chrono;

void countWordsInChunk(const char* buffer, size_t start, size_t end, unordered_map<string, int>& wordCount) {
    string word;

    for (size_t i = start; i < end; ++i) {
        char c = buffer[i];

        if (isalnum(c)) {
            word += c;
        }
        else if ((c == '\'' || c == '-') && !word.empty() && (i + 1 < end && isalnum(buffer[i + 1]))) {
            word += c;
        }
        else if (!word.empty()) {
            ++wordCount[word];
            word.clear();
        }
    }

    if (!word.empty()) {
        ++wordCount[word];
    }
}

void mergeWordCounts(unordered_map<string, int>& globalMap, const unordered_map<string, int>& localMap, mutex& mergeMutex) {
    lock_guard<mutex> lock(mergeMutex);
    for (const auto& pair : localMap) {
        globalMap[pair.first] += pair.second;
    }
}

int main() {
    const string filename = "fil3.txt";
    ifstream file(filename, ios::binary | ios::ate);

    size_t fileSize = file.tellg();
    file.seekg(0);

    unique_ptr<char[]> buffer(new char[fileSize]);
    file.read(buffer.get(), fileSize);
    file.close();

    const unsigned int numThreads = thread::hardware_concurrency();
    size_t chunkSize = fileSize / numThreads;

    vector<thread> threads;
    vector<unordered_map<string, int>> threadWordCounts(numThreads);
    mutex mergeMutex;
    unordered_map<string, int> globalWordCount;

    auto start = high_resolution_clock::now();

    for (unsigned int i = 0; i < numThreads; ++i) {
        size_t startIdx = i * chunkSize;
        size_t endIdx = (i == numThreads - 1) ? fileSize : (i + 1) * chunkSize;

        if (i != 0) {
            while (startIdx < fileSize && isalnum(buffer[startIdx])) {
                ++startIdx;
            }
        }

        threads.emplace_back([&, i, startIdx, endIdx]() {
            unordered_map<string, int> localMap;
            countWordsInChunk(buffer.get(), startIdx, endIdx, localMap);
            mergeWordCounts(globalWordCount, localMap, mergeMutex);
            });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    for (const auto& pair : globalWordCount) {
        cout << pair.first << ": " << pair.second << endl;
    }

    cout << "Processing time with threads: " << duration.count() << " milliseconds" << endl;

    return 0;
}
