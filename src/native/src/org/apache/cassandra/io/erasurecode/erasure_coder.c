/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dump.h"
#include <isa-l.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void initCoder(IsalCoder* pCoder, int numDataUnits, int numParityUnits)
{
    pCoder->verbose = 0;
    pCoder->numParityUnits = numParityUnits;
    pCoder->numDataUnits = numDataUnits;
    pCoder->numAllUnits = numDataUnits + numParityUnits;
}

// 0 not to verbose, 1 to verbose
void allowVerbose(IsalCoder* pCoder, int flag)
{
    pCoder->verbose = flag;
}

static void initEncodeMatrix(int numDataUnits, int numParityUnits,
    unsigned char* encodeMatrix)
{
    // Generate encode matrix, always invertible
    gf_gen_cauchy1_matrix(encodeMatrix, numDataUnits + numParityUnits, numDataUnits);

    // Generate Vandermonde matrix
    // gf_gen_rs_matrix(encodeMatrix, numDataUnits + numParityUnits, numDataUnits);
}

void initEncoder(IsalEncoder* pCoder, int numDataUnits,
    int numParityUnits)
{
    initCoder(&pCoder->coder, numDataUnits, numParityUnits);

    initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);

    // Generate gftbls from encode matrix
    ec_init_tables(numDataUnits, numParityUnits,
        &pCoder->encodeMatrix[numDataUnits * numDataUnits],
        pCoder->gftbls);

    if (pCoder->coder.verbose > 0) {
        dumpEncoder(pCoder);
    }
}

void initDecoder(IsalDecoder* pCoder, int numDataUnits,
    int numParityUnits)
{
    initCoder(&pCoder->coder, numDataUnits, numParityUnits);

    initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);
}

int encode(IsalEncoder* pCoder, unsigned char** dataUnits,
    unsigned char** parityUnits, int chunkSize)
{
    int numDataUnits = pCoder->coder.numDataUnits;
    int numParityUnits = pCoder->coder.numParityUnits;
    int i;

    for (i = 0; i < numParityUnits; i++) {
        memset(parityUnits[i], 0, chunkSize);
    }

    ec_encode_data(chunkSize, numDataUnits, numParityUnits,
        pCoder->gftbls, dataUnits, parityUnits);

    return 0;
}

int encodeUpdate(IsalEncoder* pCoder, unsigned char* newDataUnit, int fragment_index,
    unsigned char** parityUnits, int chunkSize)
{

    int numDataUnits = pCoder->coder.numDataUnits;
    int numParityUnits = pCoder->coder.numParityUnits;
    printf("Start encode update in C environment, target replace data block ID = %d\n", fragment_index);

    ec_encode_data_update(chunkSize, numDataUnits, numParityUnits, fragment_index, pCoder->gftbls, newDataUnit, parityUnits);
    printf("Encode update in C environment done\n");
    return 0;
}

// Return 1 when diff, 0 otherwise
static int compare(int* arr1, int len1, int* arr2, int len2)
{
    int i;

    if (len1 == len2) {
        for (i = 0; i < len1; i++) {
            if (arr1[i] != arr2[i]) {
                return 1;
            }
        }
        return 0;
    }

    return 1;
}

static int processErasures(IsalDecoder* pCoder, unsigned char** inputs, int* decodeIndexes, int* erasedIndexes, int numErased)
{
    int i, r, ret, index;
    int numDataUnits = pCoder->coder.numDataUnits;
    int isChanged = 0;

    for (i = 0; i < numDataUnits; i++) {
        pCoder->decodeIndex[i] = decodeIndexes[i];
    }

    for (i = 0; i < numDataUnits; i++) {
        pCoder->recoverySrc[i] = inputs[i];
    }

    clearDecoder(pCoder);

    for (i = 0; i < numErased; i++) {
        pCoder->erasedIndexes[i] = erasedIndexes[i];
        if (erasedIndexes[i] < numDataUnits) {
            pCoder->numErasedDataUnits++;
        }
    }

    pCoder->numErased = numErased;

    ret = generateDecodeMatrix(pCoder);
    if (ret != 0) {
        printf("Failed to generate decode matrix\n");
        return -1;
    }

    ec_init_tables(numDataUnits, pCoder->numErased,
        pCoder->decodeMatrix, pCoder->gftbls);

    if (pCoder->coder.verbose > 0) {
        dumpDecoder(pCoder);
    }

    return 0;
}

int decode(IsalDecoder* pCoder, unsigned char** inputs,
    int* decodeIndexes, int* erasedIndexes, int numErased,
    unsigned char** outputs, int chunkSize)
{
    int numDataUnits = pCoder->coder.numDataUnits;
    int i;

    printf("Start decoding in C environment, first element in decode index = %d, first element in erased index = %d\n", decodeIndexes[0], erasedIndexes[0]);
    processErasures(pCoder, inputs, decodeIndexes, erasedIndexes, numErased);
    printf("Target decode index: \n");
    for (i = 0; i < numDataUnits; i++) {
        printf(" %d ", pCoder->decodeIndex[i]);
    }
    printf("\nTarget recovery index: \n");
    for (i = 0; i < numErased; i++) {
        printf(" %d ", pCoder->erasedIndexes[i]);
    }
    printf("\n Start final decoding\n");
    for (i = 0; i < numErased; i++) {
        memset(outputs[i], 0, chunkSize);
    }

    ec_encode_data(chunkSize, numDataUnits, pCoder->numErased,
        pCoder->gftbls, pCoder->recoverySrc, outputs);

    return 0;
}

// Clear variables used per decode call
void clearDecoder(IsalDecoder* decoder)
{
    decoder->numErasedDataUnits = 0;
    decoder->numErased = 0;
    memset(decoder->gftbls, 0, sizeof(decoder->gftbls));
    memset(decoder->decodeMatrix, 0, sizeof(decoder->decodeMatrix));
    memset(decoder->tempMatrix, 0, sizeof(decoder->tempMatrix));
    memset(decoder->invertMatrix, 0, sizeof(decoder->invertMatrix));
    memset(decoder->erasedIndexes, 0, sizeof(decoder->erasedIndexes));
}

// Generate decode matrix from encode matrix
int generateDecodeMatrix(IsalDecoder* pCoder)
{
    int i, j, r, p;
    unsigned char s;
    int numDataUnits;

    numDataUnits = pCoder->coder.numDataUnits;

    // Construct matrix b by removing error rows
    for (i = 0; i < numDataUnits; i++) {
        r = pCoder->decodeIndex[i];
        for (j = 0; j < numDataUnits; j++) {
            pCoder->tempMatrix[numDataUnits * i + j] = pCoder->encodeMatrix[numDataUnits * r + j];
        }
    }

    gf_invert_matrix(pCoder->tempMatrix,
        pCoder->invertMatrix, numDataUnits);

    // Get decode matrix with only wanted recovery rows
    for (i = 0; i < pCoder->numErasedDataUnits; i++) {
        if (pCoder->erasedIndexes[i] < numDataUnits) {
            for (j = 0; j < numDataUnits; j++) {
                pCoder->decodeMatrix[numDataUnits * i + j] = pCoder->invertMatrix[numDataUnits * pCoder->erasedIndexes[i] + j];
            }
        }
    }
    // For non-src (parity) erasures need to multiply encode matrix * invert.
    for (p = 0; p < pCoder->numErasedDataUnits; p++) {
        if (pCoder->erasedIndexes[p] >= numDataUnits) { // A parity err
            for (i = 0; i < numDataUnits; i++) {
                s = 0;
                for (j = 0; j < numDataUnits; j++) {
                    s ^= gf_mul(pCoder->invertMatrix[j * numDataUnits + i],
                        pCoder->encodeMatrix[numDataUnits * pCoder->erasedIndexes[p] + j]);
                }

                pCoder->decodeMatrix[numDataUnits * p + i] = s;
            }
        }
    }

    return 0;
}
