  /**
   * Get bookmark-based recommendations
   */
  async getBookmarkBasedRecommendations(latitude, longitude, options = {}) {
    const params = new URLSearchParams({
      latitude: latitude.toString(),
      longitude: longitude.toString(),
      distance: (options.distance || 20.0).toString(),
      limit: (options.limit || 15).toString()
    });

    return await this.get(`/api/recommendations/bookmark-based?${params}`, {
      requireAuth: false
    });
  }

  /**
   * Get popular places
   */
