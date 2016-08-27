#include <iostream>
#include <vector>

unsigned int h(unsigned int i, unsigned int x, unsigned int m)
{
	unsigned int h = (x*x + x*x*x)*i;
	unsigned int modulo = (h % m);
	return modulo;
}

int main(void)
{
#if 0
	unsigned int x = 2013;
	const unsigned int m = 32;

	unsigned int bitset1 = h(1, x, m);
	unsigned int bitset2 = h(2, x, m);
	unsigned int bitset3 = h(3, x, m);	

	unsigned int bloomFilter =
		(1 << bitset1) |
		(1 << bitset2) |
		(1 << bitset3);

	std::cout << 
	std::cout << "Bloom Filter = " <<  std::bitset<m>(bloomFilter) << std::endl;
#endif

#if 0
	//std::vector<unsigned int> keys = { 2010, 2013, 2007};
	std::vector<unsigned int> keys = { 2010, 2013, 2007,2004 };
	const int m = 32;

	std::for_each(keys.begin(), keys.end(),
		[&](unsigned int x) {
			for(unsigned int i=1; i <=3; i++) {
				unsigned int bit = h(i, x, m);
				std::cout << bit << std::endl;
			}
			std::cout << std::endl;
		});
#endif

#if 0
	//std::vector<unsigned int> keys = { 2010, 2013, 2007};
	std::vector<unsigned int> keys = { 2013, 2010, 2007,2004, 2001, 1998 };
	const int m = 32;

	std::for_each(keys.begin(), keys.end(),
		[&](unsigned int x) {
			for(unsigned int i=1; i <=3; i++) {
				unsigned int bit = h(i, x, m);
				std::cout << bit << std::endl;
			}
			std::cout << std::endl;
		});
#endif

	std::vector<unsigned int> keys = { 2010, 2013, 2007, 2004, 3200};
	const int m = 32;

	std::for_each(keys.begin(), keys.end(),
		[&](unsigned int x) {
			for(unsigned int i=1; i <=3; i++) {
				unsigned int bit = h(i, x, m);
				std::cout << bit << std::endl;
			}
			std::cout << std::endl;
		});

	return 0;
}

