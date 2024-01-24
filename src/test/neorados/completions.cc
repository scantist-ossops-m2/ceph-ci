#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

namespace asio = boost::asio;

constexpr int max_completions = 10'000'000;
int completed = 0;

asio::io_context c;

void nested_cb() {
  if (++completed < max_completions)
    asio::post(c, &nested_cb);
}

int main(void) {
  post(c, &nested_cb);
  c.run();
  assert(completed == max_completions);
  return 0;
}
