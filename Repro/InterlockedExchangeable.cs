namespace Repro;

class InterlockedExchangeable<T>
    where T : class
{
    private object value;
    
    public InterlockedExchangeable(
        T initialValue)
    {
        value = initialValue;
    }

    public T Value => (T)value;

    public bool Update(
        T oldValue,
        T newValue)
    {
        return Interlocked.CompareExchange(
            ref value,
            newValue,
            oldValue) == oldValue;
    }

    public void UpdateOrThrow(
        Func<T, T> computeNewValue)
    {
        for(int i=0;i<1000;i++)
        {
            var original = Value;
            var newValue = computeNewValue(original);
            if (Update(
                    original,
                    newValue))
            {
                return;
            }
        }

        throw new  Exception("failed to update");
    }

    public void UpdateUnsafe(
        T newValue)
    {
        value = newValue;
    }
}