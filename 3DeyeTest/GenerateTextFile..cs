namespace _3DeyeTest
{
    public class GenerateTextFile
    {
        private const string AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ";
        private const int MaxGeneratedStringLength = 100;
        private const int BatchSize = 1000;

        public static void Generate(
            string outputPath,
            int maxNumber,
            int? parallelism = null,
            int totalLines = 100_000)
        {
            if (string.IsNullOrWhiteSpace(outputPath))
            {
                throw new ArgumentException("Output path must be provided.", nameof(outputPath));
            }

            if (maxNumber < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxNumber), "maxNumber must be >= 0.");
            }

            if (totalLines <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(totalLines), "totalLines must be > 0.");
            }

            int alignedWorkerCount = ResolveWorkerCount(parallelism);

            int linesPerWorker = totalLines / alignedWorkerCount;
            int remainder = totalLines % alignedWorkerCount;

            object writeLock = new();
            var random = new ThreadLocal<Random>(() =>
                new Random(unchecked(Environment.TickCount * 31 + Environment.CurrentManagedThreadId)));

            using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new StreamWriter(stream);

            Parallel.For(0, alignedWorkerCount, new ParallelOptions { MaxDegreeOfParallelism = alignedWorkerCount }, workerId =>
            {
                int linesForCurrentWorker = linesPerWorker + (workerId < remainder ? 1 : 0);
                if (linesForCurrentWorker == 0)
                {
                    return;
                }

                var localRandom = random.Value!;
                var batch = new List<string>(BatchSize);

                for (int i = 0; i < linesForCurrentWorker; i++)
                {
                    batch.Add(GenerateLine(localRandom, maxNumber));

                    if (batch.Count == BatchSize)
                    {
                        FlushBatch(writer, writeLock, batch);
                        batch.Clear();
                    }
                }

                if (batch.Count > 0)
                {
                    FlushBatch(writer, writeLock, batch);
                }
            });
        }

        public static void GenerateByTargetSize(
            string outputPath,
            int maxNumber,
            long targetSizeBytes,
            int? parallelism = null)
        {
            if (string.IsNullOrWhiteSpace(outputPath))
            {
                throw new ArgumentException("Output path must be provided.", nameof(outputPath));
            }

            if (maxNumber < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxNumber), "maxNumber must be >= 0.");
            }

            if (targetSizeBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(targetSizeBytes), "targetSizeBytes must be > 0.");
            }

            int alignedWorkerCount = ResolveWorkerCount(parallelism);
            object writeLock = new();
            var random = new ThreadLocal<Random>(() =>
                new Random(unchecked(Environment.TickCount * 31 + Environment.CurrentManagedThreadId)));
            using var cancellation = new CancellationTokenSource();

            using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new StreamWriter(stream);
            int newLineBytes = writer.Encoding.GetByteCount(writer.NewLine);
            long writtenBytes = 0;

            try
            {
                Parallel.For(0, alignedWorkerCount, new ParallelOptions
                {
                    MaxDegreeOfParallelism = alignedWorkerCount,
                    CancellationToken = cancellation.Token
                }, _ =>
                {
                    var localRandom = random.Value!;
                    var batch = new List<string>(BatchSize);

                    while (!cancellation.IsCancellationRequested)
                    {
                        batch.Add(GenerateLine(localRandom, maxNumber));
                        if (batch.Count == BatchSize)
                        {
                            if (!FlushBatchBySize(writer, writeLock, batch, targetSizeBytes, ref writtenBytes, newLineBytes, cancellation))
                            {
                                break;
                            }

                            batch.Clear();
                        }
                    }

                    if (batch.Count > 0 && !cancellation.IsCancellationRequested)
                    {
                        FlushBatchBySize(writer, writeLock, batch, targetSizeBytes, ref writtenBytes, newLineBytes, cancellation);
                    }
                });
            }
            catch (OperationCanceledException)
            {
                // Normal flow: generation stops when target file size is reached.
            }
        }

        private static string GenerateLine(Random random, int maxNumber)
        {
            int number = random.Next(0, maxNumber + 1);
            int textLength = random.Next(1, MaxGeneratedStringLength + 1);

            var chars = new char[textLength];
            for (int i = 0; i < textLength; i++)
            {
                chars[i] = AllowedChars[random.Next(AllowedChars.Length)];
            }

            return $"{number}.{new string(chars)}";
        }

        private static int ResolveWorkerCount(int? parallelism)
        {
            int cpuCount = Environment.ProcessorCount;
            int workerCount = parallelism ?? cpuCount;
            if (workerCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(parallelism), "parallelism must be > 0.");
            }

            int alignedWorkerCount = Math.Max(cpuCount, (workerCount / cpuCount) * cpuCount);
            return alignedWorkerCount == 0 ? cpuCount : alignedWorkerCount;
        }

        private static void FlushBatch(StreamWriter writer, object writeLock, List<string> batch)
        {
            lock (writeLock)
            {
                foreach (var line in batch)
                {
                    writer.WriteLine(line);
                }
            }
        }

        private static bool FlushBatchBySize(
            StreamWriter writer,
            object writeLock,
            List<string> batch,
            long targetSizeBytes,
            ref long writtenBytes,
            int newLineBytes,
            CancellationTokenSource cancellation)
        {
            lock (writeLock)
            {
                foreach (var line in batch)
                {
                    int lineBytes = writer.Encoding.GetByteCount(line) + newLineBytes;
                    if (writtenBytes + lineBytes > targetSizeBytes)
                    {
                        cancellation.Cancel();
                        return false;
                    }

                    writer.WriteLine(line);
                    writtenBytes += lineBytes;

                    if (writtenBytes >= targetSizeBytes)
                    {
                        cancellation.Cancel();
                        return false;
                    }
                }
            }

            return true;
        }
    }
}
