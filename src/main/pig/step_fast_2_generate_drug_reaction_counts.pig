/**
 * Copyright 2011 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * In the second stage of the pipeline, we join the datasets we created
 * in the first stage in order to compute the actual and expected counts
 * for each drug-drug-reaction triple that we observed in the dataset.
 *
 * The expected count for each triple is based on an independence
 * assumption: the probability of a particular drug-drug-reaction triple
 * appearing in an observation is simply the product of the frequencies
 * of the individual drugs and the reaction within the strata that
 * contains that observation.
 */

/**
 * Load the datasets that we generated in step 1.
 */
total_d1d2r = LOAD 'aers/total_d1d2r' USING PigStorage('$') as (
  gender: chararray, 
  age_bucket: double, 
  time_bucket: chararray,
  d1:chararray, 
  d2:chararray,
  reac:chararray,
  dr_count:int,
  total_count:int,
  d1_count:int,
  d2_count:int,
  reac_count:int 
);

/** 
 * Generate expected counts for each drug-drug-reaction triple within
 * within each strata. Note that the expected count for each triple is
 * (total) * (d1/total) * (d2/total) * (reac/total) = (d1*d2*reac)/(total*total).
 */
actual_expected = FOREACH total_d1d2r GENERATE gender, age_bucket,
    time_bucket, d1, d2, reac, dr_count as actual,
    (d1_count * d2_count * reac_count) / (1.0 * total_count * total_count) as expected;

/**
 * Finally, sum the actual and expected counts across strata, grouping them by
 * (d1, d2, reac) in order to get the overall counts for each triple.
 */
actual_expected_group = GROUP actual_expected BY (d1, d2, reac);
final = FOREACH actual_expected_group GENERATE group.d1 as d1, group.d2 as d2,
    group.reac as reac, SUM(actual_expected.actual) as actual,
    SUM(actual_expected.expected) as expected;

STORE final INTO 'aers/drugs2_reacs_actual_expected' USING PigStorage('$');
