## Beispiel einer Clusteranalyse: Hierarchisch & k-Means ##

# Pakete
library(psych)

# Daten: Blume Irias (Sepal = Kelchblatt, Petal = Blütenblatt)
head(iris)

# Deskriptive
describe(iris)

# keine fehlenden Werte -> muss nicht bereinigt werden
# unterschiedliche Skalierung -> muss standardisiert werden

# Standardisierung

iris.scaled<-cbind(scale(iris[, 1:4]), Species=iris$Species)

### Hierarchische Clusteranalyse ###
dist.iris <- dist(iris.scaled[,1:4], method ="euclidian") # altenative Methoden: maximum, manhattan, canberra, binary, minkowski

# Clustern

fit.1 <-hclust(dist.iris, method = "ward.D2") # alternative Methoden: ward.D, single, complete, average, mcquitty, median, centroid

# Dendrogram
plot(fit.1, hang=-1, labels=iris$Species, cex=.7)

# 3-Cluster-Lösung
iris$clus.1 <- cutree(fit.1, k=3)

# Cluster im Dendrogram markieren
rect.hclust(fit.1,k=3, border="red")

# Cluster mit Eigenschaften beschreiben
describeBy(iris[,1:4], group=iris$clus.1)
table(iris$clus.1, iris$Species)

### k-Means Clusteranalyse ###

# Anzahl der Cluster festlegen 
# Fehlerquadratsumme für verschiedene Clusterlösungen berechnen
wss.fit2 <- (nrow(iris.scaled[,1:4])-1)*sum(apply(iris.scaled[,1:4],2,var))
for( i in 2:15){
  wss.fit2[i] <- sum(kmeans(iris.scaled[,1:4], centers=i)$withinss)
}
# Fehlerquadratsumme gegen Anzahl Cluster plotten
plot(1:15, wss.fit2, type="b", xlab="Anzahl Cluster",
     ylab="Fehlerquadratsumme")

# 3 Cluster-Lösung mit 25 zufälligen initialen Clsuterzentren
fit.2 <- kmeans(iris.scaled[,1:4], 3, nstart=25)

# Clusterzentren

aggregate(iris.scaled, by=list(fit.2$cluster), FUN=mean)

# Clusterzugehörigkeit speichern
iris$clus.2 <- fit.2$cluster

# Cluster mit Eigenschaften beschreiben
describeBy(iris[,1:4], group=iris$clus.2)
table(iris$clus.2, iris$Species)

# Clusterlösungen vergleichen
table(iris$clus.2, iris$clus.1)
