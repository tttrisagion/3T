@startuml trisagion_system_context
!include <C4/C4_Context.puml>

!define DEVICONS https://raw.githubusercontent.com/tupadr3/plantuml-icon-font-sprites/master/devicons
!define FONTAWESOME https://raw.githubusercontent.com/tupadr3/plantuml-icon-font-sprites/master/font-awesome-5
!include DEVICONS/git.puml
!include FONTAWESOME/brain.puml
!include FONTAWESOME/robot.puml
!include FONTAWESOME/chart_line.puml
!include FONTAWESOME/user_shield.puml
!include FONTAWESOME/user_cog.puml
!include FONTAWESOME/building.puml

LAYOUT_WITH_LEGEND()
LAYOUT_TOP_DOWN()

title System Context Diagram for Trisagion Algorithmic Trading System

Person(risk_manager, "Risk Manager", "Oversees portfolio risk and performance. Receives alerts and reports.", $sprite="user_shield")
Person(quant, "Data Scientist / Quant Analyst", "Develops, trains, and deploys predictive models.", $sprite="brain")
Person(ops_team, "Operations Team", "Monitors system health and manages uptime.", $sprite="user_cog")

System_Ext(hyperliquid, "HyperLiquid Exchange", "Cryptocurrency derivatives exchange providing market data and trade execution.", $sprite="building")
System_Ext(uptime_robot, "UptimeRobot", "External 3rd-party monitoring and alerting service.", $sprite="robot")
System_Ext(source_repo, "Source Code Repository", "Git-based repository (e.g., GitHub) for code and configuration management.", $sprite="git")

System_Boundary(c1, "Trisagion Trading System") {
    System(trisagion, "Trisagion", "A proof-of-concept algorithmic trading system that uses ML-based predictions to manage a live portfolio.")
}

Rel(risk_manager, trisagion, "Uses", "Views performance and manages overall strategy")
Rel(quant, trisagion, "Manages Models for", "Deploys new AutoGluon models and analyzes performance")
Rel(ops_team, uptime_robot, "Receives Alerts from", "Responds to system outages and performance degradation")

Rel(trisagion, hyperliquid, "Executes Trades and Gets Market Data", "WebSocket & REST API")
Rel(trisagion, uptime_robot, "Sends Health Status to", "HTTP/S")
Rel(trisagion, source_repo, "Is Deployed From", "CI/CD Pipeline")

@enduml
