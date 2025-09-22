package io.symplify

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonProperty

data class MessageRecord(
  @JsonProperty("brand_id")
  val brandId: String,
  @JsonProperty("player_id")
  val playerId: String
) {
    private val additionalProperties: MutableMap<String, Any?> = mutableMapOf()
    
    @JsonAnyGetter
    fun getAdditionalProperties(): Map<String, Any?> = additionalProperties
    
    @JsonAnySetter
    fun setAdditionalProperty(name: String, value: Any?) {
        additionalProperties[name] = value
    }
}
