# spark-lda-coherence
Example of Topic Coherence Calculation for LDA (Latent Dirichlet allocation) model in Apache Spark

The example uses Pointwise mutual information (PMI) for topic coherence calculation.
In more details, it uses Intrinsic UMass measure.
Helpful articles:
- [Method explanation](http://qpleple.com/topic-coherence-to-evaluate-topic-models/)
- [Clarification](https://stats.stackexchange.com/questions/375062/how-does-topic-coherence-score-in-lda-intuitively-makes-sense)
- [Original Article](http://dirichlet.net/pdf/mimno11optimizing.pdf)


## How to use
There is example of usage in CoherenceTest file.

Also, you can compile the project and add it dependency to your project:

Example, publish to local Maven:
```sbt publishM2```

And next:

```libraryDependencies += "io.github.gnupinguin" %% "ldacoherence_2.12" % "1.0"```
