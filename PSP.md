# Project Structure Plan
List of deliverables, todos and issues tracker

## Main deliverables and status
| **Name**         | **Description**                                         | **Status**      |
|------------------|--------------------------------------------------------|-----------------|
| Data model       | Create data source                                    | done           |
| SQL code         | Refine | done     |
| Scoring Model    | Finalize - describe                                   | done     |
| Presentation     | Slides (?) and recording                               | next            |
| Mngt Summary           | 1 pager                                          | done
| Report           | Detailed analysis up to 3 pages                        | done            |
| CSV File         | With user_id and target group and perk                 | done            |


## ToDos
| **Topic**        | **Description**                                         | **Status**      |
|------------------|--------------------------------------------------------|-----------------|
| Data model       | - Define and escribe<br>- Clean data<br>- Feature engineering<br>- Session level and user-level                                   | postponed            |
| SQL code         | Working draft. Refine.                                 | done     |
| Scoring Model    | Design, refine, test. Document in XLS                                 | done     |
| EDA              | 10 main questions about business, target groups, ...   | done            |
| Presentation     | 5 slides, management target group, key results, obstacles, <br>limitations, further analysis, recommendations, video recording | NEXT            |
| Report           | Closer look on model and so on, including EDA          | done            |
| RFM scoring      | Do in 2nd approach                                     | postponed       |
| k-means          | Do in 2nd approach, learn from Belinda                 | postponed       |
| Mngt Dashboard   | Do in 2nd approach, evtl. with Looker                  | postponed       |

## Issues
| **Issue ID** | **Description**                                         | **By when** | **Status**      |
|--------------|---------------------------------------------------------|----------|-----------------|
| 1            | Setup github                                            | thursday| done            |
| 2            | Create readme.md                                        |       friday   | done            |
| 3            | Upload + link all deliverables                          | friday | In progress            |
| 4            | Handling missing values                                 |  | postponed            |
| 5            | Optimize scoring + draw conclusions<br>check if input for scoring needs scaling | wednesday | done            |
| 6            | Target group analysis (4 charts template) structure     | wednesday | donedone            |
| 7            | create a session-based sql script and dataframe + link final_target_group info into it. create. this is basis for number 6 detail analysis -> look if target groups "really" differentiate | wednesday | done            |
| 8            | think about small groups -> drop? -> no. chance for marketing  | wednesday | done            |
| 9           | analysis of 3 KPIs over last years -> show for presentation -> bookings, new customers, some ratios like (conversion, cancelation, etc.)                                  | wednesday | done
| 10           | Create final CSV                                  | wednesday | done
| 11           | Check python code                                  | thursday | Open
| 12           | Create Storyline + Presentation                                  | thursday | open
| 13           | Create Mngt.Summary (1-pager)                                  | friday | done
| 14          | Create 3 Pager                                  | friday | open
| 15           | Create a corr-map for features                                  |  | postponed
| 16           | Think about correlations between groups                                  |  | postponed
| 17           | EDA: Overall summary statistics, tabular, visual, verbal                                  | wednesday | done
| 18           | Describe data model                                  | thursday | postponed
| 19           | Visualize retention program for presentation and for webpage                                  | thursday | postponed




## Details zu Deliverables

**Abgabefrist**: Freitag bis spätestens 23:59 Uhr.
1. **Projektdateien**
Reichen Sie einen Link zu einem Projektordner (z.B. auf GitHub) ein, der Folgendes enthält:
   * Readme-Datei: Eine gut organisierte Readme-Datei im Markdown-Format.
   * CSV-Datei: Die CSV-Datei mit den den Vorteilen zugeordneten Benutzern.
   * Code-Ordner: Strukturierte Ordner für Quellcode (/src), Konfigurationsdateien (/config), Jupyter Notebooks (/ipynb) etc.
Dokumentation: Sorgen Sie für eine gründliche Dokumentation Ihres Codes mit Kommentaren, um die Nachvollziehbarkeit für technische Kollegen zu gewährleisten.
1. **Projektzusammenfassung**
Reichen Sie eine PDF-Datei ein, die Folgendes umfasst:
   * Executive Summary: Eine prägnante, einseitige Zusammenfassung für das Management.
   * Detaillierter Bericht: Eine ausführlichere Erklärung (bis zu 3 Seiten), die auf der Zusammenfassung aufbaut.
1. **Videopräsentation**
Nehmen Sie eine maximal 5-minütige Videopräsentation Ihrer Ergebnisse für die Unternehmensführung auf.
Inhaltliche Richtlinien:
   * Struktur: Beginnen Sie mit einem kurzen Kontext, präsentieren Sie die wichtigsten Ergebnisse pro Geschäftsfrage und beenden Sie mit einer Zusammenfassung der wichtigsten Erkenntnisse.
   * Fokus: Erzählen Sie eine fesselnde Geschichte mit Ihren Dateneinblicken. Vermeiden Sie zu viele technische Details.
   * Folien-Design: Verwenden Sie minimalen Text auf den Folien (nicht mehr als 1-2 Diagramme oder sechs Aufzählungspunkte pro Folie).
   * Klarheit: Erwähnen Sie alle getroffenen Annahmen oder Unregelmäßigkeiten in den Daten, die für das Verständnis der Ergebnisse relevant sind.