from pyprepbuddy.package import Package


class ClassNames(object):
    SPLIT_PLAN = Package.TRANSFORMERS + ".SplitPlan"
    MERGE_PLAN = Package.TRANSFORMERS + ".MergePlan"
    REPLACEMENT_FUNCTION = Package.TRANSFORMERS + ".ReplacementFunction"
    WEIGHTS = Package.SMOOTHERS + ".Weights"
    WEIGHTED_MOVING_AVERAGE = Package.SMOOTHERS + ".WeightedMovingAverageMethod"
    SIMPLE_MOVING_AVERAGE = Package.SMOOTHERS + ".SimpleMovingAverageMethod"
    NAIVE_BAYES_SUBSTITUTION = Package.IMPUTATION + ".NaiveBayesSubstitution"
    UNIVARIATE_SUBSTITUTION = Package.IMPUTATION + ".UnivariateLinearRegressionSubstitution"
    APPROX_MEAN_SUBSTITUTION = Package.IMPUTATION + ".ApproxMeanSubstitution"
    MEAN_SUBSTITUTION = Package.IMPUTATION + ".MeanSubstitution"
    MODE_SUBSTITUTION = Package.IMPUTATION + ".ModeSubstitution"
    STRING_TO_BYTES = Package.CONNECTOR + ".StringToBytes"
    CLUSTER = Package.CLUSTER + ".Cluster"
    CLUSTERS = Package.CLUSTER + ".Clusters"
    SIMPLE_FINGERPRINT = Package.CLUSTER + ".SimpleFingerprintAlgorithm"
    N_GRAM_FINGERPRINT = Package.CLUSTER + ".NGramFingerprintAlgorithm"
    FACET = Package.CLUSTER + ".TextFacets"
    DECIMAL_SCALING_NORMALIZER = Package.NORMALIZERS + ".DecimalScalingNormalizer"
    MIN_MAX_NORMALIZER = Package.NORMALIZERS + ".MinMaxNormalizer"
    Z_SCORE_NORMALIZER = Package.NORMALIZERS + ".ZScoreNormalizer"
    JAVATYPES = Package.APIJAVA + ".types"
    FileType = JAVATYPES + ".FileType"
    TRANSFORMABLE_RDD = Package.APIJAVA + ".JavaTransformableRDD"
    BYTES_TO_STRING = Package.PYTHON_CONNECTOR + ".BytesToString"
