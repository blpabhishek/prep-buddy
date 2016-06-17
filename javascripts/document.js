
var appendAll = function() {
	var deduplicateExplanation = 'In the sample dataset given above, there are 2 records with repetition. So, after calling the deduplicate method, the deduplicatedRDD will hold the following values:'
	appendParagraph(deduplicateExplanation);
	
	var deduplicateOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
							'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010']
	appendCode(deduplicateOutput);

	appendHeading('duplicates()', 'h4');
	var aboutDuplicates = 'It gives a new TransformableRDD with only the records which has a duplicate entry in the given rdd';
	appendParagraph(aboutDuplicates);

	appendHeading('Example:', 'h4');
	var duplicateCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
						'TransformableRDD duplicates = initialRDD.duplicates();',
						'duplicates.saveAsTextFile("output");']
	appendCode(duplicateCode);					
	appendParagraph('It will produce the following result:');

	var duplicateOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011'];
	appendCode(duplicateOutput);
	
	appendHeading('replace(int columnIndex, Replacemet...replacement) :', 'h4');
	var aboutReplace = 'The replace function is used when you want to replace a particular column value to a new value. You have to pass the column Index along with the one or more replacement.';
	appendParagraph(aboutReplace);
	appendHeading('Example:', 'h4');
	var replaceCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
						'TransformableRDD transformedRDD = initialRDD.replace(2, new Replacement("Missed", 0));',
						'transformedRDD.saveAsTextFile("output");'];
	appendCode(replaceCode)
	appendHeading('Output:', 'h4');
	var replaceOutput = ['07681546436,07289049655,0,11,Sat Sep 18 01:54:03 +0100 2010',
						'07681546436,07289049655,0,11,Sat Sep 18 01:54:03 +0100 2010',
						'07122915122,07220374233,0,0,Sun Oct 24 08:13:45 +0100 2010',
						'07166594208,07577423566,0,24,Thu Jan 27 14:23:39 +0000 2011',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
						'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
						'07456622368,07331532487,0,24,Sat Sep 18 13:34:09 +0100 2010'];
	appendCode(replaceOutput);

	// appendHeading('splitColumn(int columnIndex, ColumnSplitter columnSplitConfig)', 'h4');
	// appendParagraph('It splits a particular column by a given configuration. There are two types of column splitting operation available');
	// appendHeading('SplitByDelimiter');
	// appendParagraph(' takes a string delimiter and a boolean value flagging whether to retain the original column or not.');

	appendHeading('listFacets(int columnIndex):','h4');
	appendParagraph('In dataset we want to know the number of occurrences for each value of any field.');
	appendParagraph('Let say we want facets on direction column. So we just need to pass the column index of direction field to listFacets. It returns a TextFacets object.');
	var facetCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
						'TextFacets facets = initialRDD.listFacets(2);',
						'System.out.println(facets.highest());']
	appendCode(facetCode);
	appendParagraph('Output of the above code is:');
	appendCode(['(Missed, 4)']);

	appendHeading('flag(String symbol, new MarkerPredicate):', 'h4');
	appendParagraph('Flag is useful when we want mark rows as a favorite or important row.So that we can perform some operation on those rows');
	appendParagraph('Let say we want to mark those row on sample dataset whose field "direction" is "Outgoing" by "#" symbol.  Here is the code for it:');
	var flagCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
					'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
					'TransformableRDD marked = initialRDD.flag("#", new MarkerPredicate(){',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;@Override',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;public boolean evaluate(RowRecord row) {',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;String direction = row.get(2);',
						'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return direction.equals("Outgoing");',
					'&nbsp;&nbsp;&nbsp;&nbsp;}',	
					'});',	
					'marked.saveAsTextFile("flagged-data");'];
	appendCode(flagCode);
	appendParagraph('It will save the file as "flagged-data" with the following records:');
	var flagOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
						'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
						'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010,',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
						'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
						'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010,',
						'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010,'
						];
	appendCode(flagOutput);

	appendHeading('mapByFlag(String symbol, int symbolColumnIndex, Function mapFunction) :', 'h4');
	appendParagraph('We want map only on marked rows ');
	var mapByFlagCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
						'TransformableRDD marked = initialRDD.flag("#", new MarkerPredicate(){',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override',
							'&nbsp;&nbsp;&nbsp;&nbsppublic boolean evaluate(RowRecord row) {',
							'&nbsp&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbspString direction = row.get(2);',
							'&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbspreturn direction.equals("Outgoing");',
						'}',	
						'});',
						'TransformableRDD mapped = marked.mapByFlag("#", 5, new Function<String, String>(){',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override',
							'&nbsp;&nbsp;&nbsp;&nbsppublic String call(String row) throws Exception {',
								'&nbsp;&nbsp;&nbsp;&nbsp&nbsp;&nbsp;&nbsp;&nbspReturn "+" + row;',
						'&nbsp;&nbsp;&nbsp;&nbsp;}',
						'});',
						'mapped.saveAsTextFile("marked-data");'
						];
	appendCode(mapByFlagCode);
	appendParagraph('The above code will write file which will be look like:');
	var mapByFlagOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
							'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010,',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010,',
							'+07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
							'+07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011,#',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010,',
							'07456622368,07331532487,Missed,24,Sat Sep 18 13:34:09 +0100 2010,'
							];
	appendCode(mapByFlagOutput);

	appendHeading('removeRows(RowPurger.Predicate predicate):', 'h4');
	appendParagraph('It is useful when we want to remove rows from dataset for a given condition.');
	appendParagraph('Let say we want to remove those row whose field “direction” is Missed.Here is the code:');

	var removeRowCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
						'TransformableRDD purged =  initialRDD.removeRows(new RowPurger.Predicate(){',
							'&nbsp;&nbsp;&nbsp;&nbsp@Override',
							'&nbsp;&nbsp;&nbsp;&nbsppublic Boolean evaluate(RowRecord row) {',
							'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbspreturn row.get(2).equals("Missed");',
						'&nbsp;&nbsp;&nbsp;&nbsp;}',
						'});',
						'purged.saveAsTextFile("output");'
						];
	appendCode(removeRowCode);
	appendParagraph('In above code will write file which will be look like:');
	var removeRowOutput = ['07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010'
							];
	appendCode(removeRowOutput);

	appendHeading('clusters(int columnIndex, ClusteringAlgorithm algorithm)');
	appendParagraph('In this method we have to pass the clustering algorithm by which we want to group the similar item in given column index.');
	appendParagraph('We are introducing three algorithm for clustering');
	var imputationList = ['Simple Fingerprint algorithm',
							'N-Gram Fingerprint algorithm',
							'Levenshtein distance algorithm'
							];
	appendList(imputationList);
	appendHeading('By Simple fingerprint algorithm:', 'h4');
	appendCode(['Clusters clusters = transformedRDD.clusters(2 ,new SimpleFingerprintAlgorithm());']);
	appendHeading('By N-Gram Fingerprint algorithm :', 'h4');
	appendParagraph('In this algorithm we pass the value of n which is the size of chars of the token.')
	appendCode(['Clusters clusters = transformedRDD.clusters(2 ,new NGramFingerprintAlgorithm(2));']);
	appendHeading('By Levenshtein distance algorithm:');
	appendParagraph('This algorithm groups the item of field if distance between them is very less.  ');
	appendCode(['Clusters clusters = transformedRDD.clusters(2 ,new LevenshteinDistance());']);
	
	appendHeading('impute(int columnIndex,  ImputationStrategy strategy)', 'h4');
	appendParagraph('It takes column index and a stategy ImputationStrategy. This method replaces the missing value with the value returned by the strategy.');
	appendParagraph('We have a sample dataset missingCalls.csv: ');
	var imputationDataset = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
				'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
				'07122915122,07220374233,Missed,24,Sun Oct 24 08:13:45 +0100 2010',
				'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
				'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
				'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
				'07456622368,07331532487, Missed,  ,Sat Sep 18 13:34:09 +0100 2010'
					];
	appendCode(imputationDataset);
	appendParagraph('In this sample data, we want to impute at duration field.');
	appendHeading('Imputation by mean:', 'h3');
	var imputeByMeanCode = ['JavaRDD<String> callDataset= sc.textFile("missingCalls.csv");',
							'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
							'TransformableRDD imputedRDD = initialRDD.impute(3, new MeanStrategy());',
							'ImputedRDD.saveAsTextFile("output");'
								];
	appendCode(imputeByMeanCode);
	appendHeading('output:', 'h3');
	var imputeByMeanOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
							'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
							'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
							'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
							'07456622368,07331532487, Missed,<spam class="light">16.57</spam>,Sat Sep 18 13:34:09 +0100 2010'
								];
	appendCode(imputeByMeanOutput);

	appendHeading('Impute by approx mean :', 'h4');
	var imputeByApproxMeanCode = ['JavaRDD<String> callDataset= sc.textFile("missingCalls.csv");',
									'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
									'TransformableRDD imputedRDD = initialRDD.impute(3, new ApproxMeanStrategy());',
									'ImputedRDD.saveAsTextFile("output");'
									];
	appendCode(imputeByApproxMeanCode);
	
	appendHeading('output:', 'h4');
	var imputeByApproxMeanOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
									'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
									'07122915122,07220374233,Missed,0,Sun Oct 24 08:13:45 +0100 2010',
									'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
									'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
									'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
									'07456622368,07331532487, Missed,<spam class="light">16.57</spam>,Sat Sep 18 13:34:09 +0100 2010'
										];
	appendCode(imputeByApproxMeanOutput);									
	appendHeading('Impute by mode:', 'h3');
	var imputeByModeCode = ['JavaRDD<String> callDataset= sc.textFile("missingCalls.csv");',
							'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
							'TransformableRDD imputedRDD = initialRDD.impute(3, new ModeSubstitution());',
							'ImputedRDD.saveAsTextFile("output");'
							];
	appendCode(imputeByModeCode);						
	appendHeading('output:', 'h3');
	var imputeByModeOutput = ['07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
								'07681546436,07289049655,Missed,11,Sat Sep 18 01:54:03 +0100 2010',
								'07122915122,07220374233,Missed,24,Sun Oct 24 08:13:45 +0100 2010',
								'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
								'07166594208,07577423566,Outgoing,24,Thu Jan 27 14:23:39 +0000 2011',
								'07102745960,07720520621,Incoming,22,Tue Oct 12 14:16:16 +0100 2010',
								'07456622368,07331532487, Missed,<spam class="light">24</spam>,Sat Sep 18 13:34:09 +0100 2010'
								];
	appendCode(imputeByModeOutput);	

	appendHeading('Impute by naive bayes classifier:', 'h3');
	appendParagraph("This imputation is helpful when we want to predict the categorical field's missing value");
	appendParagraph('We have dataset called people.csv');
	var naiveBayesDataset = ['Name,Over 170CM, Eye, Hair length, Sex',
								'Drew,No,Blue,Short,Male', 
								'Claudia,Yes,Brown,Long,Female', 
								'Drew,No,Blue,Long,Female', 
								'Drew,No,Blue,Long,Female',
								'Alberto,Yes,Brown,Short,Male',
								'<spam style="color:rebeccapurple">Drew,Yes,Blue,Long,</spam>',
								'Karin,No,Blue,Long,Female', 
								'Nina,Yes,Brown,Short,Female', 
								'Sergio,Yes,Blue,Long,Male'
								];
	appendCode(naiveBayesDataset);
	appendParagraph('We want to predict the missing value of sex field.');
	var naiveBayesCode = ['JavaRDD<String> callDataset= sc.textFile("people.csv");',
							'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
							'TransformableRDD imputedRDD = initialRDD.impute(4, new NaiveBayesSubstitution());',
							'ImputedRDD.saveAsTextFile("output");'
							];
	appendCode(naiveBayesCode);
	appendHeading('Output:', 'h3');
	var naiveBayesOutput = ['Name,Over 170CM, Eye, Hair length, Sex',
							'Drew,No,Blue,Short,Male', 
							'Claudia,Yes,Brown,Long,Female', 
							'Drew,No,Blue,Long,Female', 
							'Drew,No,Blue,Long,Female',
							'Alberto,Yes,Brown,Short,Male',
							'Drew,Yes,Blue,Long,Female',
							'Karin,No,Blue,Long,Female',
							'Nina,Yes,Brown,Short,Female', 
							'Sergio,Yes,Blue,Long,Male'
								];
	appendCode(naiveBayesOutput);

	appendHeading('Type Inference:', 'h3');
	appendParagraph('When you don’t know the type of a particular column you can infer the dataType of it.It has varitey of type which are  useful.');
	appendHeading('Example:', 'h3');
	var typeInferCode = ['JavaRDD<String> callDataset= sc.textFile("calls.csv");',
						'TransformableRDD initialRDD = new TransformableRDD(callDataset);',
						'DataType datatype = initialRDD.inferType(1);',
						'System.out.println(datatype);'
							];
	appendCode(typeInferCode)
	appendHeading('Output:', 'h3');
	appendCode(['MOBILE_NUMBER']);						


}



$(document).ready(appendAll)