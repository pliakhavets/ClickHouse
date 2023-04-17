#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

/// TODO ensure/require that no individual chunk is empty ... otherwise reading stops

/// SourceFromChunks is the bigger brother of SourceFromSingleChunk. It supports multiple chunks and totals/extremes.
class SourceFromChunks : public IProcessor
{
public:
    /// Disambiguates the meaning of the "other" parameter in the ctor
    enum class OtherTag
    {
        Totals,
        Extremes
    };

    /// Ctors where the chunks are owned by SourceFromChunks
    SourceFromChunks(Block header, Chunks chunks_);
    SourceFromChunks(Block header, Chunks chunks_, Chunks other, OtherTag other_tag);
    SourceFromChunks(Block header, Chunks chunks_, Chunks totals_, Chunks extremes_);

    /// Ctors where the chunks are owned by someone else
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_);
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> other_, OtherTag other_tag);
    SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> totals_, std::shared_ptr<Chunks> extremes_);

    ~SourceFromChunks() override = default;

    String getName() const override;

    Status prepare() override;
    void work() override;

    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;
    std::optional<ReadProgress> getReadProgress() final;
    void progress(size_t read_rows, size_t read_bytes);

private:
    Chunk generate();

    /// ReadProgressCounters read_progress;
    /// bool read_progress_was_set = false;

    OutputPort & output;
    OutputPort * const output_totals = nullptr;
    OutputPort * const output_extremes = nullptr;

    bool has_input = false;
    bool finished = false;
    /// bool got_exception = false;

    enum class ChunkType
    {
        Output,
        Totals,
        Extremes
    };
    Port::Data port_data;
    ChunkType chunk_type;

    const std::shared_ptr<Chunks> chunks;
    const std::shared_ptr<Chunks> chunks_totals;
    const std::shared_ptr<Chunks> chunks_extremes;

    Chunks::iterator it;
    std::optional<Chunks::iterator> it_totals;
    std::optional<Chunks::iterator> it_extremes;

    const bool move_from_chunks; /// Optimization: if the chunks are exclusively owned by SourceFromChunks, then generate() can move from them
};

}
