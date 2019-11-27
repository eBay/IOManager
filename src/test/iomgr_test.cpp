#include "iomgr_test.hpp"

extern "C" {
#include <sys/epoll.h>
#include <sys/eventfd.h>
}

namespace iomgr {
extern uint32_t num_ep;
extern uint32_t num_threads;
extern uint32_t num_pri;
extern uint64_t num_attemps;
extern uint64_t wait_delta_ms;
extern uint64_t num_ev_fd;

test_ep::test_ep(std::shared_ptr< iomgr::ioMgr > iomgr_ptr) : iomgr::EndPoint(iomgr_ptr) {}

void test_ep::init_local() {}

void test_ep::print_perf() {}

void test_ep::shutdown_local() {}

IOMgrTester::IOMgrTester() {}

void IOMgrTester::start() {
    srand(time(0));
    m_cb_cnt = 0;
    m_iomgr = std::make_shared< iomgr::ioMgr >(num_ep, num_threads);
    m_ev_fd.reserve(num_ev_fd);
    for (size_t i = 0; i < num_ev_fd; i++) {
        m_ev_fd[i] = eventfd(0, EFD_NONBLOCK);
        LOGDEBUG("iomgr->add_fd: {}", m_ev_fd[i]);
        m_iomgr->add_fd(
            m_ev_fd[i], [this](auto fd, auto cookie, auto event) { process_ev_callback(fd, cookie, event); }, EPOLLIN,
            9, nullptr);
    }
    m_ep = new test_ep(m_iomgr);
    m_iomgr->add_interface(m_ep);

    m_iomgr->start();
    uint64_t temp = 1;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    if (num_attemps > 0) { [[maybe_unused]] auto wsize = write(rand_fd(), &temp, sizeof(uint64_t)); }
}

int IOMgrTester::rand_fd() { return m_ev_fd[std::rand() % num_ev_fd]; }

IOMgrTester::~IOMgrTester() {
    //
    // m_ep will be deleted by iomgr
    //
}

void IOMgrTester::stop() { m_iomgr->stop(); }

void IOMgrTester::process_ev_callback(const int fd, const void* cookie __attribute__((unused)), const int event) {
    std::lock_guard< std::mutex > lg(m_lk);
    count_cb(fd);

    uint64_t              temp;
    [[maybe_unused]] auto rsize = read(fd, &temp, sizeof(uint64_t));

    m_iomgr->process_done(fd, event);

    if (m_cb_cnt < num_attemps) {

        // trigger another event
        if (m_ev_tri_type == EvtTriggerType::TYPE_1) {
            m_iomgr->fd_reschedule(fd, event);
        } else if (m_ev_tri_type == EvtTriggerType::TYPE_2) {
            auto pick_fd = rand_fd();
            LOGINFO("cnt: {}, trigger event on fd: {} by thread: {}.", m_cb_cnt, pick_fd, get_thread_id_linear());
            [[maybe_unused]] auto wsize = write(pick_fd, &temp, sizeof(uint64_t));
        } else {
            // wrong type
            LOGERROR("Wrong event trigger type: {}", (uint64_t)m_ev_tri_type);
            assert(0);
        }
    } else {
        // we are done
        LOGINFO("Sucessfully completed all required: {} attemps. Exiting...", get_cb_cnt());
    }
}

void IOMgrTester::count_cb(int fd) {
    m_cb_cnt++;
    LOGINFO("Receiving callback: {} via fd: {}, thread: {}.", m_cb_cnt, fd, get_thread_id_linear());
}

void IOMgrTester::set_ev_tri_type(EvtTriggerType t) { m_ev_tri_type = t; }

bool IOMgrTester::wait_for_result(const uint64_t timeout_secs) {
    auto start = std::chrono::steady_clock::now();
    while (m_cb_cnt != num_attemps) {
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_delta_ms));
        auto end = std::chrono::steady_clock::now();
        auto wait_num_secs = std::chrono::duration_cast< std::chrono::seconds >(end - start).count();
        if ((uint64_t)wait_num_secs >= timeout_secs) { return false; }
    }
    return true;
}

uint64_t IOMgrTester::get_cb_cnt() const { return m_cb_cnt; }

std::size_t IOMgrTester::get_thread_id_linear() noexcept {
    static std::size_t                                        thread_idx = 0;
    static std::mutex                                         thread_mutex;
    static std::unordered_map< std::thread::id, std::size_t > thread_ids;

    std::lock_guard< std::mutex > lock(thread_mutex);
    std::thread::id               id = std::this_thread::get_id();
    auto                          iter = thread_ids.find(id);
    if (iter == thread_ids.end()) {
        iter = thread_ids.insert(std::pair< std::thread::id, std::size_t >(id, thread_idx++)).first;
    }
    return iter->second;
}

std::size_t IOMgrTester::get_thread_id() noexcept {
    static std::atomic< std::size_t > thread_idx{0};
    thread_local std::size_t          id = thread_idx;
    thread_idx++;
    return id;
}

} // namespace iomgr
