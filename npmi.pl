#!/usr/bin/perl


open(PMI, "msd_sid_voca_big_19-II-npmi.txt");
my %pmi;
while (<PMI>) {
  chomp();
  my ($w1, $w2, $value) = split /\t/, $_;
  $pmi{"$w1 $w2"} = $value;
}
close(PMI);

#open(TOPICS, "msd_sid_voca_rows.txt");
open(TOPICS, "msd_100.txt");
my $i = 1;
while (<TOPICS>) {
  chomp();
  my @terms = split / +/,$_;

  # Calculate average PMI for all unique word pairs
  # Consider weighting by prominence in topic?
  print "$i\t";
  for ($jj = 0; $jj < 19; $jj++) {

      my $npmi = 0;
      my $n = 1;
      my $numTerms = $jj;
      for ($j = 0; $j < $numTerms ; $j++) {
         $t1 = $terms[$j];
         for ($k = $j+1; $k < $numTerms; $k++) {
           $t2 = $terms[$k];
           if ($pmi{"$t1 $t2"} > 0) { 
              $npmi += $pmi{"$t1 $t2"};
              $n++;
           }
        }
      }
      if ($npmi > 0) {
         $npmi /= $n;
         print "$npmi\t";
      }
  }
  print "$_ \n";
   
  $i++;
}

close(TOPICS);

#love play time rock town movie car record awesome cover city remember place big night story radio school young 
