#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <cctype>
#include <vector>
#include <algorithm>

using namespace std;
using namespace chrono;

void countWordsInChunk(const char* buffer, size_t size, unordered_map<string, int>& wordCount, string& leftoverWord) {
    string word = leftoverWord;
    leftoverWord.clear();

    for (size_t i = 0; i < size; ++i) {
        char c = buffer[i];

        if (isalnum(c)) {
            word += c;
        }
        else if ((c == '\'' || c == '-') && !word.empty() && (i + 1 < size && isalnum(buffer[i + 1]))) {
            word += c;
        }
        else if (!word.empty()) {
            ++wordCount[word];
            word.clear();
        }
    }

    leftoverWord = word;
}

int main() {
    const string filename = "file5.txt";
    ifstream file(filename, ios::binary);

    unordered_map<string, int> wordCount;
    const size_t bufferSize = 2 * 1024 * 1024;
    unique_ptr<char[]> buffer(new char[bufferSize]);

    string leftoverWord;

    auto start = high_resolution_clock::now();

    while (file.read(buffer.get(), bufferSize) || file.gcount() > 0) {
        size_t bytesRead = file.gcount();
        countWordsInChunk(buffer.get(), bytesRead, wordCount, leftoverWord);
    }

    if (!leftoverWord.empty()) {
        ++wordCount[leftoverWord];
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    for (const auto& pair : wordCount) {
        cout << pair.first << ": " << pair.second << endl;
    }

    cout << "Processing time: " << duration.count() << " milliseconds" << endl;

    return 0;
}