using Raven.Client.Documents.Session;
using System;

namespace Hangfire.Raven.Extensions
{
    public static class IDocumentSessionExtensions
    {
        private static IMetadataDictionary GetMetadataForId<T>(this IDocumentSession session, string id)
        {
            return session.Advanced.GetMetadataFor<T>(session.Load<T>(id));
        }

        private static IMetadataDictionary GetMetadataForObject<T>(this IDocumentSession session, T obj)
        {
            return session.Advanced.GetMetadataFor<T>(obj);
        }

        public static void SetExpiry<T>(this IDocumentSession session, string id, TimeSpan expireIn)
        {
            SetExpiry(session.GetMetadataForId<T>(id), expireIn);
        }

        public static void SetExpiry<T>(this IDocumentSession session, T obj, TimeSpan expireIn)
        {
            SetExpiry(session.GetMetadataForObject<T>(obj), expireIn);
        }

        public static void SetExpiry<T>(this IDocumentSession session, T obj, DateTime expireAt)
        {
            SetExpiry(session.GetMetadataForObject<T>(obj), expireAt);
        }

        private static void SetExpiry(IMetadataDictionary metadata, DateTime expireAt)
        {
            metadata["@expires"] = expireAt.ToString("O");
        }

        private static void SetExpiry(IMetadataDictionary metadata, TimeSpan expireIn)
        {
            metadata["@expires"] = (DateTime.UtcNow + expireIn).ToString("O");
        }

        public static void RemoveExpiry<T>(this IDocumentSession session, string id)
        {
            RemoveExpiry(session.GetMetadataForId<T>(id));
        }

        public static void RemoveExpiry<T>(this IDocumentSession session, T obj)
        {
            RemoveExpiry(session.GetMetadataForObject<T>(obj));
        }

        public static void RemoveExpiry(IMetadataDictionary metadata)
        {
            metadata.Remove("@expires");
        }

        public static DateTime? GetExpiry<T>(this IDocumentSession session, string id)
        {
            return GetExpiry(session.GetMetadataForId<T>(id));
        }

        public static DateTime? GetExpiry<T>(this IDocumentSession session, T obj)
        {
            return GetExpiry(session.GetMetadataForObject<T>(obj));
        }

        private static DateTime? GetExpiry(IMetadataDictionary metadata)
        {
            return metadata.ContainsKey("@expires") ? new DateTime?(DateTime.Parse(metadata["@expires"].ToString())) : new DateTime?();
        }
    }
}
