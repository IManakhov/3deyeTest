namespace _3DeyeTest
{
    public static class Utils
    {
        public static bool TryParseSizeToBytes(string value, out long bytes)
        {
            bytes = 0;
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            string normalized = value.Trim().ToLowerInvariant();
            long multiplier = 1;
            string numberPart = normalized;

            if (normalized.EndsWith("kb"))
            {
                multiplier = 1024L;
                numberPart = normalized[..^2];
            }
            else if (normalized.EndsWith("mb"))
            {
                multiplier = 1024L * 1024L;
                numberPart = normalized[..^2];
            }
            else if (normalized.EndsWith("gb"))
            {
                multiplier = 1024L * 1024L * 1024L;
                numberPart = normalized[..^2];
            }
            else if (normalized.EndsWith("k"))
            {
                multiplier = 1024L;
                numberPart = normalized[..^1];
            }
            else if (normalized.EndsWith("m"))
            {
                multiplier = 1024L * 1024L;
                numberPart = normalized[..^1];
            }
            else if (normalized.EndsWith("g"))
            {
                multiplier = 1024L * 1024L * 1024L;
                numberPart = normalized[..^1];
            }
            else
            {
                return false;
            }

            if (!long.TryParse(numberPart.Trim(), out long valuePart) || valuePart <= 0)
            {
                return false;
            }

            try
            {
                bytes = checked(valuePart * multiplier);
                return true;
            }
            catch (OverflowException)
            {
                return false;
            }
        }
    }
}
