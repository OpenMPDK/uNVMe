#ifndef __EAGLE_HASH_IP_H__
#define __EAGLE_HASH_IP_H__

/*
class EagleHashIP
{
private:
	//					Eagle KV Hash IP Function								//
	// ============================================================================= //
	//

		Hash Input : key   :  hash input (1B ~ 512B / 1KB / 4KB / 8KB length, 128bit parallelisim/width)
					len   :  byte length of key (ex: 136bit input => len : 17) (14bit width)
					seed  :  arbitrary value (64bit width)

		Hash Output: out	 :  2 x 32bit / 2 x 64bit / 2 x 128 bit

	//
	static const unsigned int P128 = 16;

	static const unsigned long long CRC64_Table[256];
	static const unsigned char sbox[16][16];
	static const unsigned char invsbox[16][16];
	static const unsigned char Alternative_sbox[16][16];
	static const unsigned char Alternative_invsbox[16][16];
	static const unsigned char new_sbox2[256];
	static const unsigned char new_sbox3[256];
	static const unsigned char new_sbox4[256];
	static const unsigned char new_sbox5[256];


	static void Hash32_1_P128(const void* key, int len, unsigned int seed, void* out);
	static void Hash32_2_P128(const void* key, int len, unsigned int seed, void* out);

	static void Hash64_1_P128(const void* key, int len, unsigned int seed, void* out);
	static void Hash64_2_P128(const void* key, int len, unsigned int seed, void* out);
public:
	static void Hash128_1_P128(const void* key, int len, unsigned int seed, void* out);
	static void Hash128_2_P128(const void* key, int len, unsigned int seed, void* out);

};
*/

void Hash128_1_P128(const void* key, int len, unsigned int seed, void* out);
void Hash128_2_P128(const void* key, int len, unsigned int seed, void* out);


#endif
