#include <Processors/Sources/SourceFromChunks.h>

#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_)
    : IProcessor({}, {header})
    , output(*outputs.begin())
    , output_totals(nullptr)
    , output_extremes(nullptr)
    , chunks(std::make_shared<Chunks>(std::move(chunks_)))
    , chunks_totals(nullptr)
    , chunks_extremes(nullptr)
    , it(chunks->begin())
    , it_totals(std::nullopt)
    , it_extremes(std::nullopt)
    , move_from_chunks(true)
{
}

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_, Chunks other, OtherTag other_tag)
    : IProcessor({}, {header, header})
    , output(*outputs.begin())
    , output_totals(other_tag == OtherTag::Totals ? &*++outputs.begin() : nullptr)
    , output_extremes(other_tag == OtherTag::Totals ? nullptr : &*++outputs.begin())
    , chunks(std::make_shared<Chunks>(std::move(chunks_)))
    , chunks_totals(other_tag == OtherTag::Totals ? std::make_shared<Chunks>(std::move(other)) : nullptr)
    , chunks_extremes(other_tag == OtherTag::Totals ? nullptr : std::make_shared<Chunks>(std::move(other)))
    , it(chunks->begin())
    , it_totals(other_tag == OtherTag::Totals ? std::make_optional<Chunks::iterator>(chunks_totals->begin()) : std::nullopt)
    , it_extremes(other_tag == OtherTag::Totals ? std::nullopt : std::make_optional<Chunks::iterator>(chunks_extremes->begin()))
    , move_from_chunks(true)
{
}

SourceFromChunks::SourceFromChunks(Block header, Chunks chunks_, Chunks totals_, Chunks extremes_)
    : IProcessor({}, {header, header, header})
    , output(*outputs.begin())
    , output_totals(&*++outputs.begin())
    , output_extremes(&*++++outputs.begin())
    , chunks(std::make_shared<Chunks>(std::move(chunks_)))
    , chunks_totals(std::make_shared<Chunks>(std::move(totals_)))
    , chunks_extremes(std::make_shared<Chunks>(std::move(extremes_)))
    , it(chunks->begin())
    , it_totals(std::make_optional<Chunks::iterator>(chunks_totals->begin()))
    , it_extremes(std::make_optional<Chunks::iterator>(chunks_extremes->begin()))
    , move_from_chunks(true)
{
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_)
    : IProcessor({}, {header})
    , output(*outputs.begin())
    , output_totals(nullptr)
    , output_extremes(nullptr)
    , chunks(chunks_)
    , chunks_totals(nullptr)
    , chunks_extremes(nullptr)
    , it(chunks->begin())
    , it_totals(std::nullopt)
    , it_extremes(std::nullopt)
    , move_from_chunks(false)
{
    chassert(chunks);
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> other_, OtherTag other_tag)
    : IProcessor({}, {header, header})
    , output(*outputs.begin())
    , output_totals(other_tag == OtherTag::Totals ? &*++outputs.begin() : nullptr)
    , output_extremes(other_tag == OtherTag::Totals ? nullptr : &*++outputs.begin())
    , chunks(chunks_)
    , chunks_totals(other_tag == OtherTag::Totals ? other_ : nullptr)
    , chunks_extremes(other_tag == OtherTag::Totals ? nullptr : other_)
    , it(chunks->begin())
    , it_totals(other_tag == OtherTag::Totals ? std::make_optional<Chunks::iterator>(chunks_totals->begin()) : std::nullopt)
    , it_extremes(other_tag == OtherTag::Totals ? std::nullopt : std::make_optional<Chunks::iterator>(chunks_extremes->begin()))
    , move_from_chunks(false)
{
    chassert(chunks);
    if (other_tag == OtherTag::Totals)
        chassert(chunks_totals);
    else
        chassert(chunks_extremes);
}

SourceFromChunks::SourceFromChunks(Block header, std::shared_ptr<Chunks> chunks_, std::shared_ptr<Chunks> totals_, std::shared_ptr<Chunks> extremes_)
    : IProcessor({}, {header, header, header})
    , output(*outputs.begin())
    , output_totals(&*++outputs.begin())
    , output_extremes(&*++++outputs.begin())
    , chunks(chunks_)
    , chunks_totals(totals_)
    , chunks_extremes(extremes_)
    , it(chunks->begin())
    , it_totals(std::make_optional<Chunks::iterator>(chunks_totals->begin()))
    , it_extremes(std::make_optional<Chunks::iterator>(chunks_extremes->begin()))
    , move_from_chunks(false)
{
    chassert(chunks);
    chassert(chunks_totals);
    chassert(chunks_extremes);
}

String SourceFromChunks::getName() const
{
    return "SourceFromChunks";
}

SourceFromChunks::Status SourceFromChunks::prepare()
{
    if (finished)
    {
        output.finish();
        if (output_totals)
            output_totals->finish();
        if (output_extremes)
            output_extremes->finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        /// || (output_totals && !output_totals->canPush())
        /// || (output_extremes && !output_extremes->canPush()))
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    switch (chunk_type)
    {
        case ChunkType::Output:
            output.pushData(std::move(port_data));
            break;
        case ChunkType::Totals:
            output_totals->pushData(std::move(port_data));
            break;
        case ChunkType::Extremes:
            output_extremes->pushData(std::move(port_data));
            break;
    }
    has_input = false;

    /// if (isCancelled())
    /// {
    ///     output.finish();
    ///     return Status::Finished;
    /// }
    ///
    /// if (got_exception)
    /// {
    ///     finished = true;
    ///     output.finish();
    ///     return Status::Finished;
    /// }

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void SourceFromChunks::work()
{
    try
    {
        /// read_progress_was_set = false;

        if (auto chunk = generate())
        {
            port_data.chunk = std::move(chunk);
            if (port_data.chunk)
            {
                has_input = true;
                /// if (!read_progress_was_set)
                ///     progress(port_data.chunk.getNumRows(), port_data.chunk.bytes());
            }
        }
        else
            finished = true;

        /// if (isCancelled())
        ///     finished = true;
    }
    catch (...)
    {
        /// finished = true;
        /// got_exception = true;
        /// throw;
    }
}

void SourceFromChunks::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & /*storage_limits_*/)
{
    /// Ehm, should we bother?
}

void SourceFromChunks::progress(size_t /*read_rows*/, size_t /*read_bytes*/)
{
    /// read_progress_was_set = true;
    /// read_progress.read_rows += read_rows;
    /// read_progress.read_bytes += read_bytes;
}

std::optional<SourceFromChunks::ReadProgress> SourceFromChunks::getReadProgress()
{
    return {};
    /// if (finished && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
    ///     return {};
    ///
    /// ReadProgressCounters res_progress;
    /// std::swap(read_progress, res_progress);
    ///
    /// static StorageLimitsList empty_limits;
    /// return ReadProgress{res_progress, empty_limits};
}

Chunk SourceFromChunks::generate()
{
    if (it != chunks->end())
    {
        chunk_type = ChunkType::Output;
        if (move_from_chunks)
        {
            Chunk && chunk = std::move(*it);
            it++;
            return chunk;
        }
        else
        {
            Chunk chunk = it->clone();
            it++;
            return chunk;
        }
    }
    else if (it_totals.has_value() && *it_totals != chunks_totals->end())
    {
        chunk_type = ChunkType::Totals;
        if (move_from_chunks)
        {
            Chunk && chunk = std::move(**it_totals);
            (*it_totals)++;
            return chunk;
        }
        else
        {
            Chunk chunk = (*it_totals)->clone();
            (*it_totals)++;
            return chunk;
        }
    }
    else if (it_extremes.has_value() && *it_extremes != chunks_extremes->end())
    {
        chunk_type = ChunkType::Extremes;
        if (move_from_chunks)
        {
            Chunk && chunk = std::move(**it_extremes);
            (*it_extremes)++;
            return chunk;
        }
        else
        {
            Chunk chunk = (*it_extremes)->clone();
            (*it_extremes)++;
            return chunk;
        }
    }
    else
        return {};
}

}

