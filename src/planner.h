#pragma once

#include "./query.h"
#include "./scan.h"

//class Planner {
//private:
//  Query query;
//  std::shared_ptr<ResourceManager> resourceManager;
//public:
//  Planner(Query query, std::shared_ptr<ResourceManager> rm) : query{ query }, resourceManager{ rm } {};
//
//  std::unique_ptr<Scan> createPlan();
//};