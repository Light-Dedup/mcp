#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <dirent.h>
#include <libgen.h>

#include <string.h>

#include <iostream>
#include <string>
#include <queue>
#include <thread>

using namespace std;

// perror(std::format("File {}, line {}:", __FILE__, __LINE__).c_str());

// 755
// const mode_t dir_mode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
// 644
// const mode_t file_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

#define EXIT_ON(fname, is_err)\
	do {						\
		if (is_err) {			\
			perror(fname);		\
			exit(1);			\
		}						\
	} while (0)

void handle_regular_file(
	priority_queue<
		pair<size_t, int>,
		vector<pair<size_t, int> >,
		greater<pair<size_t, int> >
	> &pq,
	vector<vector<pair<int, int> > > &jobs,
	struct dirent *de,
	int dst_dir_fd)
{
	struct stat statbuf;
	int src_fd = open(de->d_name, O_RDONLY);
	EXIT_ON("open", src_fd == -1);
	EXIT_ON("stat", fstat(src_fd, &statbuf) == -1);
	int dst_fd = openat(dst_dir_fd, de->d_name,
		O_CREAT | O_TRUNC | O_WRONLY, statbuf.st_mode);
	auto data = pq.top();
	pq.pop();
	jobs[data.second].emplace_back(src_fd, dst_fd);
	data.first += statbuf.st_size;
	pq.push(data);
}
void handle_symbolic_link(
	int dst_fd,
	struct dirent *de)
{
	constexpr int len = 256;
	char target[len];
	ssize_t ret = readlink(de->d_name, target, len);
	EXIT_ON("readlink", ret == -1);
	if (ret < len) {
		EXIT_ON("symlink", symlinkat(target, dst_fd, de->d_name) == -1);
		return;
	}
	string target_str(target, len);
	do {
		ret = readlink(de->d_name, target, len);
		EXIT_ON("readlink", ret == -1);
		target_str.append(target, ret);
	} while (ret == len);
	EXIT_ON("symlink", symlinkat(target_str.c_str(), dst_fd, de->d_name) == -1);
}
void cp_dir(
	priority_queue<
		pair<size_t, int>,
		vector<pair<size_t, int> >,
		greater<pair<size_t, int> >
	> &pq,
	vector<vector<pair<int, int> > > &jobs,
	DIR *src,
	int dst_fd)
{
	struct dirent *de;
	while ((de = readdir(src)) != NULL) {
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
		{
			continue;
		}
		switch (de->d_type) {
		case DT_REG:
			handle_regular_file(pq, jobs, de, dst_fd);
			break;
		case DT_LNK:
			handle_symbolic_link(dst_fd, de);
			break;
		case DT_DIR:
			int src_fd2 = open(de->d_name, O_DIRECTORY);
			struct stat dir_stat;
			EXIT_ON("fstat", fstat(src_fd2, &dir_stat) == -1);
			if (mkdirat(dst_fd, de->d_name, dir_stat.st_mode) == -1)
				EXIT_ON("mkdirat", errno != EEXIST);
			int dst_fd2 = openat(dst_fd, de->d_name, O_DIRECTORY);
			EXIT_ON("openat", dst_fd == -1);
			EXIT_ON("fchdir", fchdir(src_fd2) == -1);
			DIR *src2 = fdopendir(src_fd2);
			EXIT_ON("opendir", src2 == NULL);
			cp_dir(pq, jobs, src2, dst_fd2);
			EXIT_ON("chdir", chdir("..") == -1);
			EXIT_ON("closedir", closedir(src2) == -1);
			EXIT_ON("close", close(dst_fd2) == -1);
			break;
		// Ignore others
		}
	}
}

void writen(int fd, void *_buf, size_t n) {
	char *buf = (char *)_buf;
	while (n) {
		ssize_t ret = write(fd, buf, n);
		EXIT_ON("write", ret == -1);
		n -= ret;
		buf += ret;
	}
}
void cp_func(const vector<pair<int, int> > &jobs, size_t buf_len) {
	void *buf = malloc(buf_len);
	EXIT_ON("malloc", buf == NULL);
	for (auto job : jobs) {
		int src_fd = job.first;
		int dst_fd = job.second;
		while (1) {
			ssize_t ret = read(src_fd, buf, buf_len);
			EXIT_ON("read", ret < 0);
			if (ret == 0)
				break;
			writen(dst_fd, buf, ret);
		}
		EXIT_ON("close", close(src_fd) == -1);
		EXIT_ON("close", close(dst_fd) == -1);
	}
}

int main(int argc, char **argv) {
	if (argc < 4) {
		cout << "Usage: " << argv[0] <<
			" src_dir dst_dir thread_num [buf_bytes]" << endl;
		return 0;
	}
	int thread_num = atoi(argv[3]);
	size_t buf_len;
	if (argc < 5) {
		buf_len = 4096;
	} else {
		buf_len = atoi(argv[4]);
	}
	int src_fd = open(argv[1], O_DIRECTORY);
	EXIT_ON("open", src_fd == -1);
	struct stat dir_stat;
	EXIT_ON("fstat", fstat(src_fd, &dir_stat) == -1);
	int dst_fd = open(argv[2], O_DIRECTORY);
	if (dst_fd == -1) {
		if (errno != ENOENT)
			EXIT_ON("open", true);
		EXIT_ON("mkdir", mkdir(argv[2], dir_stat.st_mode) == -1);
		dst_fd = open(argv[2], O_DIRECTORY);
		EXIT_ON("open", dst_fd == -1);
	} else {
		char *name = basename(argv[1]);
		EXIT_ON("mkdirat",
			mkdirat(dst_fd, name, dir_stat.st_mode) == -1 && errno != EEXIST);
		dst_fd = openat(dst_fd, name, O_DIRECTORY);
		EXIT_ON("open", dst_fd == -1);
	}
	EXIT_ON("fchdir", fchdir(src_fd) == -1);
	DIR *src = fdopendir(src_fd);
	EXIT_ON("opendir", src == NULL);

	priority_queue<
		pair<size_t, int>,
		vector<pair<size_t, int> >,
		greater<pair<size_t, int> >
	> pq;
	for (int i = 0; i < thread_num; ++i) {
		pq.emplace(0, i);
	}
	vector<vector<pair<int, int> > > jobs(thread_num);
	cp_dir(pq, jobs, src, dst_fd);
	EXIT_ON("closedir", closedir(src) == -1);
	EXIT_ON("close", close(dst_fd) == -1);

	vector<thread> threads;
	for (int i = 0; i < thread_num; ++i) {
		threads.emplace_back(cp_func, jobs[i], buf_len);
	}
	for (int i = 0; i < thread_num; ++i) {
		threads[i].join();
	}

	return 0;
}
