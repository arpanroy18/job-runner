CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -Wpedantic -std=c++20
LDFLAGS ?= -pthread

SRC := $(wildcard src/*.cpp src/*/*.cpp)
OBJ := $(SRC:src/%.cpp=build/%.o)

jr: $(OBJ)
	$(CXX) $(OBJ) -o $@ $(LDFLAGS)

build/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf build jr

.PHONY: clean
