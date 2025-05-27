#include <mpi.h>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <cctype>
#include <chrono>
#include <memory>
#include <cstring>

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
    if (!word.empty()) ++wordCount[word];
}

void mergeWordCounts(unordered_map<string, int>& globalMap, const unordered_map<string, int>& localMap, mutex& mergeMutex) {
    lock_guard<mutex> lock(mergeMutex);
    for (const auto& pair : localMap) {
        globalMap[pair.first] += pair.second;
    }
}

int main(int argc, char** argv) {
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, nullptr);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    const string filename = "file5.txt";
    MPI_File fh;
    MPI_File_open(MPI_COMM_WORLD, filename.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
    MPI_Offset fileSize;
    MPI_File_get_size(fh, &fileSize);

    MPI_Win win;
    char* shared_buffer = nullptr;
    MPI_Win_allocate_shared(rank == 0 ? fileSize : 0, sizeof(char), MPI_INFO_NULL, MPI_COMM_WORLD, &shared_buffer, &win);
    if (rank != 0) {
        int disp_unit;
        MPI_Aint shared_size;
        MPI_Win_shared_query(win, 0, &shared_size, &disp_unit, &shared_buffer);
    }

    if (rank == 0) {
        MPI_File_read_at(fh, 0, shared_buffer, fileSize, MPI_CHAR, MPI_STATUS_IGNORE);
    }
    MPI_File_close(&fh);
    MPI_Barrier(MPI_COMM_WORLD);

    size_t chunkSize = fileSize / size;
    size_t startIdx = rank * chunkSize;
    size_t endIdx = (rank == size - 1) ? fileSize : (rank + 1) * chunkSize;

    if (rank != 0) {
        while (startIdx > 0 && isalnum(shared_buffer[startIdx - 1])) {
            --startIdx;
        }
    }

    if (rank != size - 1) {
        while (endIdx < fileSize && isalnum(shared_buffer[endIdx])) {
            ++endIdx;
        }
    }

    const unsigned int numThreads = thread::hardware_concurrency();
    size_t threadChunkSize = (endIdx - startIdx) / numThreads;

    unordered_map<string, int> rankWordCount;
    mutex mergeMutex;

    auto t1 = high_resolution_clock::now();

    vector<thread> threads;
    for (unsigned int i = 0; i < numThreads; ++i) {
        size_t tStart = startIdx + i * threadChunkSize;
        size_t tEnd = (i == numThreads - 1) ? endIdx : (tStart + threadChunkSize);
        if (i != 0) {
            while (tStart > startIdx && isalnum(shared_buffer[tStart - 1])) {
                --tStart;
            }
        }
        if (i != numThreads - 1) {
            while (tEnd < endIdx && isalnum(shared_buffer[tEnd])) {
                ++tEnd;
            }
        }

        threads.emplace_back([&, tStart, tEnd]() {
            unordered_map<string, int> localCount;
            countWordsInChunk(shared_buffer, tStart, tEnd, localCount);
            mergeWordCounts(rankWordCount, localCount, mergeMutex);
            });
    }

    for (auto& t : threads) t.join();

    vector<char> localSerialized;
    size_t local_size = 0;
    for (auto& [word, cnt] : rankWordCount)
        local_size += sizeof(uint32_t) + word.size() + sizeof(int);

    localSerialized.resize(local_size);
    char* ptr = localSerialized.data();
    for (auto& [word, cnt] : rankWordCount) {
        uint32_t len = word.size();
        memcpy(ptr, &len, sizeof(uint32_t)); ptr += sizeof(uint32_t);
        memcpy(ptr, word.data(), len); ptr += len;
        memcpy(ptr, &cnt, sizeof(int)); ptr += sizeof(int);
    }

    vector<int> recvCounts(size);
    int localSize = localSerialized.size();
    MPI_Gather(&localSize, 1, MPI_INT, recvCounts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);

    vector<int> displs(size);
    vector<char> recvBuffer;
    if (rank == 0) {
        int totalSize = 0;
        for (int i = 0; i < size; ++i) {
            displs[i] = totalSize;
            totalSize += recvCounts[i];
        }
        recvBuffer.resize(totalSize);
    }

    MPI_Gatherv(localSerialized.data(), localSize, MPI_CHAR,
        recvBuffer.data(), recvCounts.data(), displs.data(), MPI_CHAR, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        unordered_map<string, int> finalWordCount;
        int offset = 0;
        for (int i = 0; i < size; ++i) {
            char* p = recvBuffer.data() + offset;
            char* end = p + recvCounts[i];
            while (p < end) {
                uint32_t len; memcpy(&len, p, sizeof(uint32_t)); p += sizeof(uint32_t);
                string word(p, len); p += len;
                int cnt; memcpy(&cnt, p, sizeof(int)); p += sizeof(int);
                finalWordCount[word] += cnt;
            }
            offset += recvCounts[i];
        }

        auto t2 = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(t2 - t1);

        for (auto& [word, cnt] : finalWordCount)
            cout << word << ": " << cnt << "\n";

        cout << "MPI-3 Processing time: " << duration.count() << " ms\n";
    }

    MPI_Win_free(&win);
    MPI_Finalize();
    return 0;
}
