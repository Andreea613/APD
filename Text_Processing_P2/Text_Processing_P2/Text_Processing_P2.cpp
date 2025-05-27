#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <sstream>
#include <cctype>
#include <vector>
#include <memory>
#include <chrono>

using namespace std;
using namespace chrono;

void countWordsInChunk(const string& data, unordered_map<string, int>& wordCount, string& leftover) {
    string word = leftover;
    leftover.clear();

    for (char c : data) {
        if (isalnum(c)) {
            word += c;
        }
        else if ((c == '\'' || c == '-') && !word.empty()) {
            word += c;
        }
        else if (!word.empty()) {
            ++wordCount[word];
            word.clear();
        }
    }

    leftover = word;
}

string serializeMap(const unordered_map<string, int>& wordCount) {
    stringstream ss;
    for (const auto& pair : wordCount) {
        ss << pair.first << " " << pair.second << "\n";
    }
    return ss.str();
}

unordered_map<string, int> deserializeMap(const string& data) {
    unordered_map<string, int> map;
    stringstream ss(data);
    string word;
    int count;
    while (ss >> word >> count) {
        map[word] += count;
    }
    return map;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    const string filename = "file.txt";
    MPI_File file;
    MPI_Offset fileSize;

    unordered_map<string, int> localWordCount;
    const MPI_Offset bufferSize = 2 * 1024 * 1024;

    MPI_File_open(MPI_COMM_WORLD, filename.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &file);
    MPI_File_get_size(file, &fileSize);

    MPI_Offset chunkSize = fileSize / size;
    MPI_Offset startOffset = rank * chunkSize;
    if (rank == size - 1)
        chunkSize += fileSize % size;

    vector<char> buffer(chunkSize + 100);

    MPI_File_read_at(file, startOffset, buffer.data(), chunkSize + 100, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&file);

    string leftover;
    string data(buffer.begin(), buffer.begin() + chunkSize + 100);
    auto start = high_resolution_clock::now();

    countWordsInChunk(data, localWordCount, leftover);
    if (!leftover.empty()) {
        ++localWordCount[leftover];
    }

    string serialized = serializeMap(localWordCount);
    int sendSize = serialized.size();

    if (rank == 0) {
        unordered_map<string, int> globalCount = localWordCount;
        for (int i = 1; i < size; ++i) {
            int recvSize;
            MPI_Recv(&recvSize, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            string recvBuf(recvSize, '\0');
            MPI_Recv(recvBuf.data(), recvSize, MPI_CHAR, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            auto partialMap = deserializeMap(recvBuf);
            for (const auto& [word, count] : partialMap)
                globalCount[word] += count;
        }

        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);

        for (const auto& [word, count] : globalCount) {
            cout << word << ": " << count << endl;
        }

        cout << "Total time: " << duration.count() << " milliseconds" << endl;
    }
    else {
        MPI_Send(&sendSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(serialized.c_str(), sendSize, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
