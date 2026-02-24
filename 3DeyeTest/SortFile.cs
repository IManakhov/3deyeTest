namespace _3DeyeTest
{
    public class SortFile
    {
        private const int DefaultChunkSizeLines = 200_000;

        public static void Sort(string inputPath, int? parallelism = null)
        {
            if (string.IsNullOrWhiteSpace(inputPath))
            {
                throw new ArgumentException("Input path must be provided.", nameof(inputPath));
            }

            if (!File.Exists(inputPath))
            {
                throw new FileNotFoundException("Input file not found.", inputPath);
            }

            int workerCount = ResolveWorkerCount(parallelism);
            string tempDirectory = Path.Combine(Path.GetTempPath(), $"3DeyeTest-sort-{Guid.NewGuid():N}");
            string tempOutputPath = Path.Combine(tempDirectory, "sorted-output.tmp");
            Directory.CreateDirectory(tempDirectory);

            var chunkTasks = new List<Task<string>>();
            var throttle = new SemaphoreSlim(workerCount, workerCount);

            try
            {
                using (var reader = new StreamReader(inputPath))
                {
                    int chunkIndex = 0;
                    while (!reader.EndOfStream)
                    {
                        var chunkLines = new List<string>(DefaultChunkSizeLines);
                        for (int i = 0; i < DefaultChunkSizeLines && !reader.EndOfStream; i++)
                        {
                            string? line = reader.ReadLine();
                            if (line is not null)
                            {
                                chunkLines.Add(line);
                            }
                        }

                        if (chunkLines.Count == 0)
                        {
                            break;
                        }

                        throttle.Wait();
                        int localChunkIndex = chunkIndex++;
                        chunkTasks.Add(Task.Run(() =>
                        {
                            try
                            {
                                return SortAndSaveChunk(tempDirectory, localChunkIndex, chunkLines);
                            }
                            finally
                            {
                                throttle.Release();
                            }
                        }));
                    }
                }

                Task.WaitAll(chunkTasks.ToArray());
                var chunkPaths = chunkTasks.Select(task => task.Result).ToArray();
                MergeChunks(chunkPaths, tempOutputPath);
                File.Move(tempOutputPath, inputPath, overwrite: true);
            }
            finally
            {
                throttle.Dispose();

                if (Directory.Exists(tempDirectory))
                {
                    Directory.Delete(tempDirectory, recursive: true);
                }
            }
        }

        private static string SortAndSaveChunk(string tempDirectory, int chunkIndex, List<string> lines)
        {
            var records = new List<SortRecord>(lines.Count);
            foreach (var line in lines)
            {
                records.Add(new SortRecord(line, BuildSortKey(line)));
            }

            records.Sort(SortRecordComparer.Instance);

            string chunkPath = Path.Combine(tempDirectory, $"chunk-{chunkIndex:D6}.txt");
            using var writer = new StreamWriter(chunkPath);
            foreach (var record in records)
            {
                writer.WriteLine(record.Line);
            }

            return chunkPath;
        }

        private static void MergeChunks(string[] chunkPaths, string outputPath)
        {
            var readers = new StreamReader[chunkPaths.Length];
            var queue = new PriorityQueue<MergeItem, SortKey>(SortKeyComparer.Instance);

            try
            {
                for (int i = 0; i < chunkPaths.Length; i++)
                {
                    readers[i] = new StreamReader(chunkPaths[i]);
                    string? line = readers[i].ReadLine();
                    if (line is null)
                    {
                        continue;
                    }

                    queue.Enqueue(new MergeItem(i, line), BuildSortKey(line));
                }

                using var writer = new StreamWriter(outputPath);
                while (queue.TryDequeue(out var item, out _))
                {
                    writer.WriteLine(item.Line);
                    string? nextLine = readers[item.ReaderIndex].ReadLine();
                    if (nextLine is not null)
                    {
                        queue.Enqueue(new MergeItem(item.ReaderIndex, nextLine), BuildSortKey(nextLine));
                    }
                }
            }
            finally
            {
                foreach (var reader in readers)
                {
                    reader?.Dispose();
                }
            }
        }

        private static SortKey BuildSortKey(string line)
        {
            int dotIndex = line.IndexOf('.');
            if (dotIndex <= 0 || dotIndex == line.Length - 1)
            {
                return new SortKey(line, int.MaxValue, line);
            }

            if (!int.TryParse(line.AsSpan(0, dotIndex), out int number))
            {
                return new SortKey(line, int.MaxValue, line);
            }

            string textPart = line[(dotIndex + 1)..];
            return new SortKey(textPart, number, line);
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

        private readonly record struct SortKey(string Text, int Number, string RawLine);
        private readonly record struct SortRecord(string Line, SortKey Key);
        private readonly record struct MergeItem(int ReaderIndex, string Line);

        private sealed class SortRecordComparer : IComparer<SortRecord>
        {
            public static SortRecordComparer Instance { get; } = new();

            public int Compare(SortRecord x, SortRecord y)
            {
                return SortKeyComparer.Instance.Compare(x.Key, y.Key);
            }
        }

        private sealed class SortKeyComparer : IComparer<SortKey>
        {
            public static SortKeyComparer Instance { get; } = new();

            public int Compare(SortKey x, SortKey y)
            {
                int textCompare = string.CompareOrdinal(x.Text, y.Text);
                if (textCompare != 0)
                {
                    return textCompare;
                }

                int numberCompare = x.Number.CompareTo(y.Number);
                if (numberCompare != 0)
                {
                    return numberCompare;
                }

                return string.CompareOrdinal(x.RawLine, y.RawLine);
            }
        }
    }
}