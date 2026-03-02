package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║       Level 10: Lifecycle Ops, Retention, and Capstone          ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "snapshot":
			SnapshotConsistencyDemo()
		case "delete":
			DeleteReclaimDemo()
		case "retention":
			RetentionIncidentDrill()
		case "refcount":
			ShowReferenceCounting()
		case "simulation":
			SimulateSnapshotMerge()
		case "compare":
			ShowForceMergeComparison()
		case "monitor":
			ShowMonitoringQueries()
		case "manual":
			ShowManualIntervention()
		case "review":
			ShowPostIncidentReview()
		default:
			fmt.Printf("Unknown command: %s\n", os.Args[1])
			printUsage()
			os.Exit(1)
		}
		return
	}

	runAllDemos()
}

func printUsage() {
	fmt.Println("Usage: lab [command]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  snapshot   - Run snapshot consistency demo")
	fmt.Println("  delete     - Run delete-reclaim experiment demo")
	fmt.Println("  retention  - Run retention incident drill")
	fmt.Println("  refcount   - Show reference counting mechanism")
	fmt.Println("  simulation - Simulate snapshot with concurrent merge")
	fmt.Println("  compare    - Compare natural merge vs force merge")
	fmt.Println("  monitor    - Show monitoring queries for delete")
	fmt.Println("  manual     - Show manual intervention options")
	fmt.Println("  review     - Show post-incident review questions")
	fmt.Println()
	fmt.Println("Run without arguments to execute all demos.")
}

func runAllDemos() {
	fmt.Println("Running all Level 10 labs...")
	fmt.Println()

	SnapshotConsistencyDemo()
	fmt.Println()
	fmt.Println("Press Enter to continue to next lab...")
	fmt.Scanln()

	DeleteReclaimDemo()
	fmt.Println()
	fmt.Println("Press Enter to continue to next lab...")
	fmt.Scanln()

	RetentionIncidentDrill()

	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════════════════════")
	fmt.Println("              Level 10 Labs Complete!")
	fmt.Println("══════════════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("Key takeaways:")
	fmt.Println("  1. Snapshots use hard links for point-in-time consistency")
	fmt.Println("  2. Reference counting prevents deletion during active use")
	fmt.Println("  3. Delete: rows disappear fast, space reclaims later")
	fmt.Println("  4. Force merge accelerates space reclamation")
	fmt.Println("  5. Retention + disk pressure protect against data loss")
	fmt.Println("  6. Read-only mode is last resort safeguard")
	fmt.Println()
	fmt.Println("Run 'lab <command>' for specific demos.")
}
