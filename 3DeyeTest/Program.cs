using System.Diagnostics;
using _3DeyeTest;

if (args.Length == 0)
{
    PrintUsage();
    return;
}

string mode = args[0].ToLowerInvariant();
switch (mode)
{
    case "generate":
        RunGenerate(args);
        break;
    case "sort":
        RunSort(args);
        break;
    default:
        WriteLine($"Unknown mode: {args[0]}");
        PrintUsage();
        break;
}

static void RunGenerate(string[] args)
{
    if (args.Length < 2 || !int.TryParse(args[1], out var maxNumber))
    {
        WriteLine("For generate mode, <maxNumber> is required.");
        PrintUsage();
        return;
    }

    string outputPath = args.Length > 2 ? args[2] : "generated.txt";
    string? sizeOrLinesArg = args.Length > 3 ? args[3] : null;
    int? parallelism = args.Length > 4 && int.TryParse(args[4], out var parsedParallelism) ? parsedParallelism : null;

    var stopwatch = Stopwatch.StartNew();

    if (!string.IsNullOrWhiteSpace(sizeOrLinesArg) && Utils.TryParseSizeToBytes(sizeOrLinesArg, out var targetSizeBytes))
    {
        GenerateTextFile.GenerateByTargetSize(outputPath, maxNumber, targetSizeBytes, parallelism);
    }
    else
    {
        int totalLines = sizeOrLinesArg is not null && int.TryParse(sizeOrLinesArg, out var parsedLines)
            ? parsedLines
            : 100_000;
        GenerateTextFile.Generate(outputPath, maxNumber, parallelism, totalLines);
    }

    stopwatch.Stop();
    WriteLine($"File generated: {outputPath}");
    WriteLine($"Generate time: {stopwatch.Elapsed}");
}

static void RunSort(string[] args)
{
    if (args.Length < 2)
    {
        WriteLine("For sort mode, <inputPath> is required.");
        PrintUsage();
        return;
    }

    string inputPath = args[1];
    int? parallelism = args.Length > 2 && int.TryParse(args[2], out var parsedParallelism) ? parsedParallelism : null;

    var stopwatch = Stopwatch.StartNew();
    SortFile.Sort(inputPath, parallelism);
    stopwatch.Stop();

    WriteLine($"File sorted: {inputPath}");
    WriteLine($"Sort time: {stopwatch.Elapsed}");
}

static void PrintUsage()
{
    WriteLine("Usage:");
    WriteLine("  dotnet run -- generate <maxNumber> [outputPath] [totalLines|targetSize] [parallelism]");
    WriteLine("  dotnet run -- sort <inputPath> [parallelism]");
    WriteLine("Examples:");
    WriteLine("  dotnet run -- generate 999 generated.txt 100000 16");
    WriteLine("  dotnet run -- generate 999 generated.txt 100mb 16");
    WriteLine("  dotnet run -- generate 999 generated.txt 1gb 16");
    WriteLine("  dotnet run -- sort generated.txt 16");
}

static void WriteLine(string message)
{
    Console.WriteLine(message);
    File.AppendAllText("output.txt", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + " " + message + Environment.NewLine);
}