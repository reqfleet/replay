package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/reqfleet/replay/internal/parser"
	"github.com/reqfleet/replay/internal/recorder"
)

func combineUsage(flagSet *flag.FlagSet, output io.Writer) {
	fmt.Fprintln(output, "usage: replay combine -log <mixed-input> -out <canonical-output> [-zstd]")
	flagSet.SetOutput(output)
	flagSet.PrintDefaults()
}

func runCombine(args []string, stdout, stderr io.Writer) int {
	return runCombineContext(context.Background(), args, stdout, stderr)
}

func runCombineContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("combine", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	logPath := flags.String("log", "", "path to mixed Envoy NDJSON input")
	outPath := flags.String("out", "", "path to plain canonical NDJSON output")
	zstdInput := flags.Bool("zstd", false, "read input compressed with zstd")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			combineUsage(flags, stdout)
			return 0
		}
		fmt.Fprintf(stderr, "combine: parse arguments: %v\n", err)
		return 2
	}
	if *logPath == "" {
		fmt.Fprintln(stderr, "combine: validate arguments: -log is required")
		return 2
	}
	if *outPath == "" {
		fmt.Fprintln(stderr, "combine: validate arguments: -out is required")
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "combine: validate arguments: unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}

	format := ""
	if *zstdInput {
		format = "zstd"
	}
	summary, err := combineFiles(ctx, *logPath, *outPath, format)
	if err != nil {
		fmt.Fprintf(stderr, "combine: %v\n", err)
		return 2
	}
	if summary.DiscardedStarts != 0 || summary.DiscardedEnds != 0 {
		fmt.Fprintf(stderr, "combine: warning: discarded unmatched observations starts=%d ends=%d\n",
			summary.DiscardedStarts, summary.DiscardedEnds)
	}
	fmt.Fprintf(stdout, "combine complete starts=%d ends=%d records=%d connections_closed=%d\n",
		summary.Starts, summary.Ends, summary.Records, summary.ConnectionsClosed)
	return 0
}

type inputOpenResult struct {
	input parser.InterruptibleReadCloser
	err   error
}

func openCombineInput(ctx context.Context, path, format string) (parser.InterruptibleReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("canceled: %w", err)
	}

	opened := make(chan inputOpenResult, 1)
	go func() {
		input, err := parser.OpenFile(path, format)
		opened <- inputOpenResult{input: input, err: err}
	}()

	select {
	case result := <-opened:
		if err := ctx.Err(); err != nil {
			if result.input != nil {
				if closeErr := result.input.Close(); closeErr != nil {
					return nil, fmt.Errorf("canceled: %w; close input: %w", err, closeErr)
				}
			}
			return nil, fmt.Errorf("canceled: %w", err)
		}
		return result.input, result.err
	case <-ctx.Done():
		go func() {
			result := <-opened
			if result.input != nil {
				_ = result.input.Close() // The command has returned; close any late result.
			}
		}()
		return nil, fmt.Errorf("canceled: %w", ctx.Err())
	}
}

func sameInputAndOutput(inputPath, outputPath string) (bool, error) {
	absoluteInput, err := filepath.Abs(inputPath)
	if err != nil {
		return false, fmt.Errorf("resolve input path: %w", err)
	}
	absoluteOutput, err := filepath.Abs(outputPath)
	if err != nil {
		return false, fmt.Errorf("resolve output path: %w", err)
	}
	if filepath.Clean(absoluteInput) == filepath.Clean(absoluteOutput) {
		return true, nil
	}

	outputInfo, err := os.Stat(outputPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect output path: %w", err)
	}
	inputInfo, err := os.Stat(inputPath)
	if err != nil {
		return false, fmt.Errorf("inspect input path: %w", err)
	}
	return os.SameFile(inputInfo, outputInfo), nil
}

func joinCleanupError(primary, cleanup error) error {
	if primary == nil {
		return cleanup
	}
	return fmt.Errorf("%w; %w", primary, cleanup)
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(data)
}

func combineFiles(ctx context.Context, inputPath, outputPath, format string) (summary recorder.CombineSummary, returnErr error) {
	same, err := sameInputAndOutput(inputPath, outputPath)
	if err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("validate paths: %w", err)
	}
	if same {
		return recorder.CombineSummary{}, fmt.Errorf("validate paths: input and output paths are identical")
	}
	if err := ctx.Err(); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("canceled: %w", err)
	}

	input, err := openCombineInput(ctx, inputPath, format)
	if err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("open input: %w", err)
	}
	inputOpen := true

	var temporary *os.File
	var temporaryPath string
	temporaryOpen := false
	installed := false
	defer func() {
		cleanupErrors := make([]string, 0, 3)
		if temporaryOpen {
			if err := temporary.Close(); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("close temporary output: %v", err))
			}
		}
		if inputOpen {
			if err := input.Close(); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("close input: %v", err))
			}
		}
		if temporaryPath != "" && !installed {
			if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("remove temporary output: %v", err))
			}
		}
		if len(cleanupErrors) == 0 {
			return
		}
		summary = recorder.CombineSummary{}
		cleanupError := errors.New(strings.Join(cleanupErrors, "; "))
		returnErr = joinCleanupError(returnErr, cleanupError)
	}()
	cancelInterruptDone := make(chan struct{})
	stopCancelInterrupt := context.AfterFunc(ctx, func() {
		_ = input.Interrupt() // Cancellation is reported through ctx.Err().
		close(cancelInterruptDone)
	})
	cancelInterruptActive := true
	finishCancelInterrupt := func() {
		if !cancelInterruptActive {
			return
		}
		cancelInterruptActive = false
		if stopCancelInterrupt() {
			return
		}
		<-cancelInterruptDone
	}
	defer finishCancelInterrupt()
	outputDir := filepath.Dir(outputPath)
	temporary, err = os.CreateTemp(outputDir, ".replay-combine-*")
	if err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath = temporary.Name()
	temporaryOpen = true
	if err := temporary.Chmod(0o600); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("set temporary output permissions: %w", err)
	}

	summary, err = recorder.CombineStream(input, contextWriter{ctx: ctx, writer: temporary})
	finishCancelInterrupt()
	if err := ctx.Err(); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("canceled: %w", err)
	}
	if err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("pair observations: %w", err)
	}
	if err := input.Close(); err != nil {
		inputOpen = false
		return recorder.CombineSummary{}, fmt.Errorf("close input: %w", err)
	}
	inputOpen = false
	if err := temporary.Sync(); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("sync output: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("canceled: %w", err)
	}
	if err := temporary.Close(); err != nil {
		temporaryOpen = false
		return recorder.CombineSummary{}, fmt.Errorf("close output: %w", err)
	}
	temporaryOpen = false
	if err := ctx.Err(); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("canceled: %w", err)
	}
	if err := os.Rename(temporaryPath, outputPath); err != nil {
		return recorder.CombineSummary{}, fmt.Errorf("install output: %w", err)
	}
	installed = true
	return summary, nil
}
