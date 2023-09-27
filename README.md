# integration-et-gestion-des-donnees-de-transport-public-avec-azure
Ce projet vise à collecter, transformer et gérer les données de transport public en utilisant les services Azure tels que Azure Data Lake Storage Gen2 et Azure Databricks.
 
## Planification :
![image](https://github.com/elouardyabderrahim/integration-et-gestion-des-donnees-de-transport-public-avec-azure/assets/101024060/2b8ff7bc-7720-4cbe-9bc1-062dac65788a)


## Description des Données

Les données originales comprennent des informations sur les voyages, notamment la date, le type de transport, l'itinéraire, l'heure de départ et d'arrivée, le nombre de passagers et les retards.

## Données Transformées

Les données ont été transformées en trois tables distinctes pour une analyse plus approfondie :

- **Table Principale**: Contient des informations détaillées sur les voyages, y compris les retards, la durée, etc.

- **Table d'Analyse des Itinéraires**: Fournit des statistiques agrégées par itinéraire, telles que le retard moyen et le nombre de passagers moyen.

- **Table d'Analyse des Heures de Pointe**: Identifie les heures de pointe en fonction du nombre de passagers et des retards.

## Transformations

Diverses transformations ont été appliquées aux données brutes pour les préparer à l'analyse, notamment l'extraction de l'année, du mois et du jour à partir de la date, la catégorisation des retards, etc.

## Lignage des Données

Les données proviennent de TransportMa et ont été traitées à l'aide d'Azure Databricks. Le lien de lignage des données est le suivant : "Données provenant de TransportMa et traitées à l'aide d'Azure Databricks."

## Directives d'Utilisation

Les données transformées peuvent être utilisées pour divers cas d'utilisation, notamment l'analyse des performances de transport, l'optimisation des itinéraires et la planification des heures de pointe. Assurez-vous de comprendre les catégories de retard pour une interprétation correcte.

## Automatisation des Politiques de Conservation

Nous avons automatisé la politique de conservation des données en archivant les fichiers les plus anciens à intervalles réguliers. De plus, des données supplémentaires sont générées à intervalles de lots pour enrichir les données existantes.

## Batch Processing

Le traitement par lots est effectué pour maintenir les données à jour. Un job est créé pour exécuter le notebook de traitement à des intervalles spécifiques, garantissant ainsi des données fraîches pour l'analyse.

## Auteur

Ce projet a été réalisé par Abderrahim Elouardy dans le cadre de Projet ETL avec Azure Databricks.

Pour toute question ou assistance, veuillez me contacter à elouardy.abderrahim07@gmail.com

Merci d'utiliser ce projet !


