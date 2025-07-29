#include <vector>
#include <cmath>
#include <numeric>
#include <algorithm>
#include <map>

// Helper function to calculate factorial on the CPU
long long factorial(int n) {
    return (n == 1 || n == 0) ? 1 : n * factorial(n - 1);
}

// We must use 'extern "C"' to prevent C++ name mangling
extern "C" {

    double calculate_cpu_perm_entropy(const double* x_host, int n, int order, int delay) {
        if (order <= 1 || order > 9) { // Keep order reasonable for CPU
            return -1.0;
        }

        int num_motifs = n - (order - 1) * delay;
        if (num_motifs <= 0) return 0.0;

        // --- Replicate antropy's hashing logic ---
        std::vector<unsigned int> hash_mult(order);
        for(int i = 0; i < order; ++i) {
            hash_mult[i] = pow(order, i);
        }

        // --- Main computation loop on the CPU ---
        std::vector<unsigned int> hash_values;
        hash_values.reserve(num_motifs);

        std::vector<double> motif(order);
        std::vector<int> indices(order);

        for (int i = 0; i < num_motifs; ++i) {
            // 1. Create the motif
            for (int j = 0; j < order; ++j) {
                motif[j] = x_host[i + j * delay];
            }

            // 2. Get permutation indices (argsort)
            std::iota(indices.begin(), indices.end(), 0); // Fill with 0, 1, 2...
            std::sort(indices.begin(), indices.end(),
                      [&](int a, int b) { return motif[a] < motif[b]; });

            // 3. Calculate the unique hash value
            unsigned int hash_val = 0;
            for (int j = 0; j < order; ++j) {
                hash_val += indices[j] * hash_mult[j];
            }
            hash_values.push_back(hash_val);
        }

        // 4. Count unique hash values (mimicking np.unique)
        std::map<unsigned int, int> counts;
        for (unsigned int val : hash_values) {
            counts[val]++;
        }

        // 5. Calculate final entropy
        double pe = 0.0;
        for (auto const& [key, val] : counts) {
            double p = static_cast<double>(val) / num_motifs;
            if (p > 0) {
                pe -= p * log2(p);
            }
        }

        // 6. Normalize
        double max_entropy = log2(static_cast<double>(factorial(order)));
        if (max_entropy > 0) {
            pe /= max_entropy;
        }

        return pe;
    }
}

