#ifndef CORO_H
#define CORO_H
#include<boost/bind.hpp>
#include<boost/coroutine/all.hpp>

using namespace boost::coroutines;

typedef symmetric_coroutine<void>::call_type coro_call_t;
typedef symmetric_coroutine<void>::yield_type coro_yield_t;
typedef int coro_id_t;
#endif
