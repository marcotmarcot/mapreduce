#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <semaphore>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

template <typename T>
class ConsumerInput {
 public:
  ConsumerInput() : semaphore_(0) {}

  void Emit(const T& output) {
    {
      std::scoped_lock lock(mutex_);
      inputs_.push_back(output);
    }
    semaphore_.release();
  }

  std::tuple<T, bool> Consume() {
    while (true) {
      semaphore_.acquire();
      std::scoped_lock lock(mutex_);
      if (inputs_.empty()) {
        return {T(), false};
      }
      T output = inputs_.back();
      inputs_.pop_back();
      return {output, true};
    }
  }

  void Close() {
    semaphore_.release();
  }

 private:
  std::vector<T> inputs_;
  std::mutex mutex_;
  std::counting_semaphore<101> semaphore_;
};

template <typename T>
class MapOutput {
 public:
  MapOutput(const int consumers) : consumers_(consumers) {
    for (int i = 0; i < consumers; ++i) {
      consumers_[i] = std::make_unique<ConsumerInput<T>>();
    }
  }

  void Emit(const T& output) {
    const std::size_t hash = std::hash<T>{}(output);
    const std::size_t index = hash % consumers_.size();
    consumers_[index]->Emit(output);
  }

  std::tuple<T, bool> Consume(const int index) {
    return consumers_[index]->Consume();
  }

  void Close() {
    for (auto& consumer : consumers_) {
      consumer->Close();
    }
  }

 private:
  std::vector<std::unique_ptr<ConsumerInput<T>>> consumers_;
};

void ReadFile(const int id, MapOutput<std::string>& output) {
  std::ifstream file(std::to_string(id));
  for (std::string line; std::getline(file, line); ) {
    output.Emit(line);
  }
}

void Map(const int id, MapOutput<std::string>& input, MapOutput<int>& output) {
  std::unordered_set<std::string> count;
  while (true) {
    auto [word, ok] = input.Consume(id);
    if (!ok) {
      break;
    }
    count.insert(word);
  }
  output.Emit(count.size());
}

void Reduce(MapOutput<int>& input, int& output) {
  output = 0;
  while (true) {
    auto [count, ok] = input.Consume(0);
    if (!ok) {
      return;
    }
    output += count;
  }
}

int main() {
  const int kNumFileReaders = 10000;
  const int kNumMappers = 1;
  MapOutput<std::string> file_reader_output(kNumMappers);
  std::vector<std::thread> file_readers;
  for (int i = 0; i < kNumFileReaders; ++i) {
    file_readers.emplace_back(&ReadFile, i, std::ref(file_reader_output));
  }

  MapOutput<int> map_output(1);
  std::vector<std::thread> mappers;
  for (int i = 0; i < kNumMappers; ++i) {
    mappers.emplace_back(&Map, i, std::ref(file_reader_output), std::ref(map_output));
  }

  int reducer_output;
  std::thread reducer(&Reduce, std::ref(map_output), std::ref(reducer_output));

  for (std::thread& thread : file_readers) {
    thread.join();
  }
  file_reader_output.Close();

  for (std::thread& thread : mappers) {
    thread.join();
  }
  map_output.Close();

  reducer.join();

  std::cout << reducer_output << std::endl;

  return 0;
}
