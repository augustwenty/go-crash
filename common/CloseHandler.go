package common
		
import (
    "fmt"
	"os"
	"syscall"
	"os/signal"
	"context"
)

var shutdownCompleteContext context.Context

// InitCloseHandler - initializes a ctrl+c handler with a signal/event upon termination
// Returns (cancellation context, shutdown complete event)
func InitCloseHandler() (context.Context, context.CancelFunc) {
	cancellationContext, cancelFunction := context.WithCancel(context.Background())
	shutdownContext, shutdownCompleteSignal := context.WithCancel(context.Background())

	shutdownCompleteContext = shutdownContext
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGKILL)

	fmt.Println("Press Ctrl+C to exit...")
	go monitorForTermination(osSignals, cancelFunction)

	return cancellationContext, shutdownCompleteSignal
}

// monitorForTermination - monitors for ctrl+c
func monitorForTermination(osSignals chan os.Signal, cancelFunction context.CancelFunc) {
	for range osSignals {
		fmt.Println("Terminating due to Ctrl+C event")
		cancelFunction()
	}
}

// WaitForShutdownComplete - waits for signal that shutdown is complete
func WaitForShutdownComplete() {
	select {
	case <- shutdownCompleteContext.Done():
		return
	}
}