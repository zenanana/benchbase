/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.distributions;

import java.util.concurrent.ThreadLocalRandom;


/**
 * Generates integers according to a discrete distribution
 */
public class DiscreteGenerator extends IntegerGenerator {
    int[] weights;
    int min, max;

    public DiscreteGenerator(int[] weights) {
        this.weights = weights;
        this.min = 0;
        this.max = 0;
        for (int weight : weights) {
            this.max += weight;
        }

        setLastInt(-1);
    }

    /**
     * If the generator returns numeric (integer) values, return the next value as an int. Default is to return -1, which
     * is appropriate for generators that do not return numeric values.
     */
    public int nextInt() {
        int randomNum = randInt(this.min, this.max);
        int sum = 0;
        int ret = -1;
        for (int i = 0; i < this.weights.length; i++) {
            sum += this.weights[i];
            if (randomNum <= sum) {
                ret = i;
                break;
            }
        }

        setLastInt(ret);
        return ret;
    }

    @Override
    public double mean() {
        throw new UnsupportedOperationException("No mean is defined for discrete distributions!");
    }

    /* Get random integer between min and max, inclusive */
    public int randInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }
}
