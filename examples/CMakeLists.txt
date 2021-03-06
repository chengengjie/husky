# Copyright 2016 Husky Team
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
endif(MONGOCLIENT_FOUND)

if(THRIFT_FOUND)
    include_directories(${THRIFT_INCLUDE_DIR})
    add_definitions(-DWITH_THRIFT)
endif(THRIFT_FOUND)

set(husky husky-lib husky-io husky-core husky-base) 

### Applications

# WordCountMR
add_executable(WordCountMR wc_mr.cpp)
target_link_libraries(WordCountMR ${husky})
target_link_libraries(WordCountMR ${EXTERNAL_LIB})
set_property(TARGET WordCountMR PROPERTY CXX_STANDARD 14)

# WordCountMongo
add_executable(WordCountMongo wc_mr_mongo.cpp)
target_link_libraries(WordCountMongo ${husky})
target_link_libraries(WordCountMongo ${EXTERNAL_LIB})
set_property(TARGET WordCountMongo PROPERTY CXX_STANDARD 14)

# WordCountFlume
add_executable(WordCountFlume wc_mr_flume.cpp)
target_link_libraries(WordCountFlume ${husky})
target_link_libraries(WordCountFlume ${EXTERNAL_LIB})
set_property(TARGET WordCountFlume PROPERTY CXX_STANDARD 14)

# PI
add_executable(PI pi.cpp)
target_link_libraries(PI ${husky})
target_link_libraries(PI ${EXTERNAL_LIB})
set_property(TARGET PI PROPERTY CXX_STANDARD 14)

# MF_BOOST
add_executable(MF_BOOST max_flow/mf_boost.cpp)
target_link_libraries(MF_BOOST ${husky})
target_link_libraries(MF_BOOST ${EXTERNAL_LIB})
set_property(TARGET MF_BOOST PROPERTY CXX_STANDARD 14)

# MF_HUSKY
add_executable(MF_HUSKY max_flow/mf_husky.cpp max_flow/edmonds_karp_prefix.cpp max_flow/edmonds_karp_predecessor.cpp
max_flow/push_relabel.cpp)
target_link_libraries(MF_HUSKY ${husky})
target_link_libraries(MF_HUSKY ${EXTERNAL_LIB})
set_property(TARGET MF_HUSKY PROPERTY CXX_STANDARD 14)

# CC
add_executable(CC connected_components.cpp)
target_link_libraries(CC ${husky})
target_link_libraries(CC ${EXTERNAL_LIB})
set_property(TARGET CC PROPERTY CXX_STANDARD 14)

# PIC
add_executable(PIC pic.cpp)
target_link_libraries(PIC ${husky})
target_link_libraries(PIC ${EXTERNAL_LIB})
set_property(TARGET PIC PROPERTY CXX_STANDARD 14)

# PageRank
add_executable(PageRank pagerank.cpp)
target_link_libraries(PageRank ${husky})
target_link_libraries(PageRank ${EXTERNAL_LIB})
set_property(TARGET PageRank PROPERTY CXX_STANDARD 14)

# Aggregator
add_executable(Aggregator aggregator.cpp)
target_link_libraries(Aggregator ${husky})
target_link_libraries(Aggregator ${EXTERNAL_LIB})
set_property(TARGET Aggregator PROPERTY CXX_STANDARD 14)

# PR_CC
add_executable(PR_CC pr_cc_attrlist.cpp)
target_link_libraries(PR_CC ${husky})
target_link_libraries(PR_CC ${EXTERNAL_LIB})
set_property(TARGET PR_CC PROPERTY CXX_STANDARD 14)

# Vector
add_executable(vector vector.cpp)
target_link_libraries(vector ${husky})
target_link_libraries(vector ${EXTERNAL_LIB})
set_property(TARGET vector PROPERTY CXX_STANDARD 14)

# LabeledPoint
add_executable(labeled_point labeled_point.cpp)
target_link_libraries(labeled_point ${husky})
target_link_libraries(labeled_point ${EXTERNAL_LIB})
set_property(TARGET labeled_point PROPERTY CXX_STANDARD 14)

# Linear Regression
add_executable(LinearRegression linear_regression.cpp)
target_link_libraries(LinearRegression ${husky})
target_link_libraries(LinearRegression ${EXTERNAL_LIB})
set_property(TARGET LinearRegression PROPERTY CXX_STANDARD 14)

# Logistic Regression
add_executable(LogisticRegression logistic_regression.cpp)
target_link_libraries(LogisticRegression ${husky})
target_link_libraries(LogisticRegression ${EXTERNAL_LIB})
set_property(TARGET LogisticRegression PROPERTY CXX_STANDARD 14)

# SVM
add_executable(SVM svm.cpp)
target_link_libraries(SVM ${husky})
target_link_libraries(SVM ${EXTERNAL_LIB})
set_property(TARGET SVM PROPERTY CXX_STANDARD 14)

# tf-idf
add_executable(TF-IDF tfidf.cpp)
target_link_libraries(TF-IDF ${husky})
target_link_libraries(TF-IDF ${EXTERNAL_LIB})
set_property(TARGET Aggregator PROPERTY CXX_STANDARD 14)

# Affinity matrix
add_executable(AMAT affinity.cpp)
target_link_libraries(AMAT ${husky})
target_link_libraries(AMAT ${EXTERNAL_LIB})
set_property(TARGET AMAT PROPERTY CXX_STANDARD 14)
